# email_utils.py
import os
import mimetypes
import smtplib
import boto3
from email.message import EmailMessage
from email.utils import formatdate, make_msgid

# -------- utilities --------
def parse_recipients(s: str | None) -> list[str]:
    """Split comma/semicolon-separated recipient string into a clean list."""
    if not s:
        return []
    parts = [p.strip() for p in s.replace(";", ",").split(",")]
    return [p for p in parts if p]

def _build_mime(subject: str, sender: str, to_addrs: list[str],
                body_text: str | None = None,
                body_html: str | None = None,
                attachments: list[str] | None = None) -> EmailMessage:
    """
    Create an EmailMessage with plain+html (if provided) and optional attachments.
    """
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(to_addrs)
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid(domain=sender.split("@")[-1])

    # Body: prefer HTML + add plain fallback; or just plain text.
    if body_html:
        # Fallback/plain part (use body_text if provided; else strip tags minimal)
        msg.set_content(body_text or "Please see the HTML part of this message.")
        msg.add_alternative(body_html, subtype="html")
    else:
        msg.set_content(body_text or "")

    # Attachments
    for path in (attachments or []):
        filename = os.path.basename(path)
        ctype = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        maintype, subtype = ctype.split("/", 1)
        with open(path, "rb") as f:
            msg.add_attachment(f.read(), maintype=maintype, subtype=subtype, filename=filename)
    return msg

# -------- SES transport (unchanged) --------
def send_email_with_attachment(
    ses_region: str,
    sender: str,
    to_addrs: list[str],
    subject: str,
    body_text: str,
    attachment_path: str,
    attachment_name: str | None = None,
) -> str | None:
    """Send one attachment via SES.SendRawEmail (kept for backward compatibility)."""
    if not to_addrs:
        return None
    if not attachment_name:
        attachment_name = os.path.basename(attachment_path)
    msg = _build_mime(subject, sender, to_addrs, body_text=body_text, attachments=[attachment_path])
    ses = boto3.client("ses", region_name=ses_region)
    resp = ses.send_raw_email(RawMessage={"Data": msg.as_bytes()})
    return resp.get("MessageId")

# -------- SMTP transport (NEW) --------
def send_email_via_smtp(
    smtp_host: str,
    smtp_port: int,
    sender: str,
    to_addrs: list[str],
    subject: str,
    body_text: str | None = None,
    body_html: str | None = None,
    attachments: list[str] | None = None,
    *,
    use_starttls: bool = False,
    username: str | None = None,
    password: str | None = None,
    helo_host: str | None = None,
    timeout: int = 60,
    dsn_notify: str | None = "SUCCESS,FAILURE",
    dsn_ret: str | None = "HDRS",
) -> str:
    """
    Send via a raw SMTP relay (e.g., 10.0.31.212:25).

    - use_starttls=False for port 25 relay (as in your JAMS job)
    - username/password optional (leave None for open relay within VPC)
    - DSN (delivery status notifications) requested if supported by server

    Returns the generated Message-ID (header).
    """
    if not to_addrs:
        raise ValueError("No recipients provided")

    msg = _build_mime(subject, sender, to_addrs, body_text=body_text, body_html=body_html, attachments=attachments)
    message_id = msg["Message-ID"]

    # Connect
    if helo_host:
        server = smtplib.SMTP(host=smtp_host, port=smtp_port, local_hostname=helo_host, timeout=timeout)
    else:
        server = smtplib.SMTP(host=smtp_host, port=smtp_port, timeout=timeout)

    try:
        server.ehlo_or_helo_if_needed()
        if use_starttls:
            server.starttls()
            server.ehlo()

        if username and password:
            server.login(username, password)

        # DSN options if server supports them
        mail_opts = []
        rcpt_opts = []
        if dsn_ret:
            mail_opts.append(f"RET={dsn_ret}")           # HDRS or FULL
        if dsn_notify:
            rcpt_opts.append(f"NOTIFY={dsn_notify}")     # e.g., SUCCESS,FAILURE

        # Envelope sender is the From
        server.sendmail(sender, to_addrs, msg.as_bytes(), mail_options=mail_opts, rcpt_options=rcpt_opts)
    finally:
        try:
            server.quit()
        except Exception:
            server.close()

    return message_id
