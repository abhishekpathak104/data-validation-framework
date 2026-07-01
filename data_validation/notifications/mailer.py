"""
data_validation/notifications/mailer.py
---------------------------------------
Handles validation failure alerting: queries notification handler text from
MySQL metadata tables and dispatches email alerts via SendGrid SMTP.

Environment variables required:
    SENDGRID_API_KEY    - SendGrid API key for SMTP authentication.
    NOTIFICATION_SENDER - Sender email address (e.g. 'alerts@example.com').
"""

import csv
import logging
import os
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
from tabulate import tabulate

logger = logging.getLogger(__name__)

# Null-like string values that indicate no criticality is set.
_NULL_CRITICALITY_VALUES = frozenset({"None", "NaN", "null", "nan", "NULL"})

_ATTACHMENT_FILENAME = "validation_errors.csv"


def notification_alert(pdf, cursor, database):
    """Query notification handler text and dispatch email alerts for failures.

    Iterates over each row in the PySpark DataFrame ``pdf``, fetches the
    configured notification handler text from the MySQL metadata tables, and
    collects rows that have a non-null criticality into a staging DataFrame.
    When at least one such row exists, triggers :func:`mail_notification`.

    Args:
        pdf: PySpark DataFrame containing validation error aggregates.  Must
            include the columns: ``Column_Name``, ``Rule_ID``, ``Object_ID``,
            ``Run_ID``, ``Criticality``, ``Object_Name``, ``Rule_Description``.
        cursor: Active MySQL database cursor used for metadata queries.
        database (str): MySQL database/schema name containing the metadata
            tables.
    """
    columns = [
        "Column_Name",
        "Rule_ID",
        "Object_ID",
        "Run_ID",
        "Criticality",
        "Object_Name",
        "Rule_Description",
    ]
    pdf = pdf.select(*columns).toPandas()
    logger.debug("Notification alert input:\n%s", pdf)

    staging_df = pd.DataFrame()
    object_id = None

    for idx in pdf.index:
        object_id = pdf["Object_ID"][idx]
        criticality = str(pdf["Criticality"][idx])

        if criticality in _NULL_CRITICALITY_VALUES:
            continue

        query = f"""
            SELECT Notification_Handler_Text
            FROM `{database}`.data_validation_notification_message_handler a
            LEFT JOIN `{database}`.data_validation_notification b
                ON a.Notification_Handler_ID = b.Notification_Handler_ID
            LEFT JOIN `{database}`.data_validation_object_notification c
                ON b.Notification_ID = c.Notification_ID
            WHERE c.ObjectID = {object_id}
              AND Criticality = {criticality}
        """
        logger.debug("Notification query: %s", query)
        cursor.execute(query)
        result = cursor.fetchall()

        handler_text = ""
        for row in result:
            handler_text = row[0]

        row_df = pdf.iloc[[idx]].copy()
        row_df.insert(len(row_df.columns), "Notification_Handler_Text", handler_text)
        staging_df = pd.concat([staging_df, row_df], ignore_index=True)

    if not staging_df.empty and object_id is not None:
        mail_notification(object_id, staging_df, cursor, database)


def _fetch_contact_info(object_id, cursor, database):
    """Fetch notification distribution list for a given object.

    Args:
        object_id (int): The object identifier to query contacts for.
        cursor: Active MySQL database cursor.
        database (str): MySQL database/schema name.

    Returns:
        tuple: A pandas DataFrame with columns
            ``[ObjectID, Contact_email, Criticality]`` and the CSV content
            string written to :data:`_ATTACHMENT_FILENAME`.
    """
    query = f"""
        SELECT
            ObjectID,
            c.Notification_Distribution_Contact AS Contact_email,
            Criticality
        FROM `{database}`.data_validation_object_notification a
        LEFT JOIN `{database}`.data_validation_notification b
            ON a.Notification_ID = b.Notification_ID
        LEFT JOIN `{database}`.data_validation_notification_distribution_list c
            ON b.Notification_Distribution_ID = c.Notification_Distribution_ID
        LEFT JOIN `{database}`.data_validation_notification_message_handler d
            ON b.Notification_Handler_ID = d.Notification_Handler_ID
        WHERE ObjectID = {object_id}
    """
    cursor.execute(query)
    contact_df = pd.DataFrame(
        cursor.fetchall(),
        columns=["ObjectID", "Contact_email", "Criticality"],
    )
    return contact_df


def mail_notification(object_id, df, cursor, database):
    """Build and send an email notification with a CSV attachment.

    Fetches the distribution list from MySQL, constructs a multipart email
    with the validation error summary as both plain-text and HTML bodies,
    and attaches the errors as a CSV file.  Sends via SendGrid SMTP.

    Credentials are read from the environment variables
    ``SENDGRID_API_KEY`` and ``NOTIFICATION_SENDER``.

    Args:
        object_id (int): Object identifier used to look up notification
            contacts.
        df (pd.DataFrame): Pandas DataFrame containing the validation errors
            to include in the notification.
        cursor: Active MySQL database cursor.
        database (str): MySQL database/schema name.
    """
    sendgrid_api_key = os.environ.get("SENDGRID_API_KEY", "")
    sender_email = os.environ.get("NOTIFICATION_SENDER", "noreply@example.com")

    if not sendgrid_api_key:
        logger.warning(
            "SENDGRID_API_KEY environment variable not set. "
            "Email notification will not be sent."
        )
        return

    contact_df = _fetch_contact_info(object_id, cursor, database)
    if contact_df.empty:
        logger.warning("No notification contacts found for object_id=%s.", object_id)
        return

    recipient_email = contact_df["Contact_email"].iloc[0]
    logger.info("Sending notification to %s for object_id=%s.", recipient_email, object_id)

    # Write errors to a temporary CSV attachment.
    df.to_csv(_ATTACHMENT_FILENAME, encoding="utf-8", index=False)

    # Build email bodies from the CSV contents.
    text_template = (
        "Data validation alert:\n\n{table}\n\nRegards,\nData Validation Team"
    )
    html_template = (
        "<html><body>"
        "<p>Hello,</p>"
        "<p>Please find the data validation alert details below:</p>"
        "{table}"
        "<p>Regards,</p>"
        "<p>Data Validation Team</p>"
        "</body></html>"
    )

    with open(_ATTACHMENT_FILENAME, newline="", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file)
        data = list(reader)

    text_body = text_template.format(
        table=tabulate(data, headers="firstrow", tablefmt="psql")
    )
    html_body = html_template.format(
        table=tabulate(data, headers="firstrow", tablefmt="html")
    )

    # Assemble MIME message.
    msg = MIMEMultipart("mixed")
    msg["From"] = sender_email
    msg["To"] = recipient_email
    msg["Subject"] = "Data Validation Notification"

    alternative_part = MIMEMultipart("alternative")
    alternative_part.attach(MIMEText(text_body, "plain"))
    alternative_part.attach(MIMEText(html_body, "html"))
    msg.attach(alternative_part)

    # Attach CSV file.
    with open(_ATTACHMENT_FILENAME, "rb") as attachment_file:
        attachment = MIMEBase("application", "octet-stream")
        attachment.set_payload(attachment_file.read())

    encoders.encode_base64(attachment)
    attachment.add_header(
        "Content-Disposition",
        "attachment",
        filename=_ATTACHMENT_FILENAME,
    )
    msg.attach(attachment)

    # Send via SendGrid SMTP.
    try:
        with smtplib.SMTP("smtp.sendgrid.net", 587) as server:
            server.ehlo()
            server.starttls()
            server.login("apikey", sendgrid_api_key)
            server.sendmail(sender_email, recipient_email, msg.as_string())
        logger.info("Notification email sent successfully to %s.", recipient_email)
    except smtplib.SMTPException as exc:
        logger.error("Failed to send notification email: %s", exc)