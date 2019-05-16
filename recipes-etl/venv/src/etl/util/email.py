
__author__ = 'arun_bonam'

#!/usr/bin/env python

import smtplib

class Gmail(object):
    def __init__(self, email, password,subject,message):
        self.email = email
        self.password = password
        self.server = 'smtp.gmail.com'
        self.port = 587
        session = smtplib.SMTP(self.server, self.port)
        session.ehlo()
        session.starttls()
        session.ehlo
        session.login(self.email, self.password)
        self.session = session
        self.subject =subject
        self.message =message
        self.send_message(subject,message)

    def send_message(self, subject, body):

        headers = [
            "From: " + self.email,
            "Subject: " + subject,
            "To: " + self.email,
            "MIME-Version: 1.0",
            "Content-Type: text/html"]
        headers = "\r\n".join(headers)
        self.session.sendmail(
            self.email,
            self.email,
            headers + "\r\n\r\n" + body)
