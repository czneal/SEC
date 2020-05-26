import smtplib
from email.message import EmailMessage

from settings import Settings


def send_message(to: str, subject: str, content: str):
    msg = EmailMessage()

    msg.set_content(content)
    msg['Subject'] = subject
    msg['From'] = "mgadmin@machinegrading.ee"
    msg['To'] = to

    # Send the message via our own SMTP server.
    s = smtplib.SMTP('localhost', port=Settings.smtp_port())
    s.send_message(msg)
    s.quit()


def main():
    send_message("vkugushev@gmail.com", subject="Parse errors",
                 content="There are few errors while parsing SEC reprts")


if __name__ == '__main__':
    main()
