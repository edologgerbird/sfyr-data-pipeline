import requests
#import json

class airflowFailureNotif: 
    def __init__(self):
        chat_id = -759507262,
        bot_token = "5260006434:AAHIBLE1yMiJPTNZOR2qjVUfTH3OxSfEkFw"
        # with open('utils/serviceAccount.json', 'r') as jsonFile:
        #     self.cred = json.load(jsonFile)

    def sendTelegramNotif(self):
        notification_text  = "%F0%9F%94%B4%20AIRFLOW%20DAG%20RUN%20FAILURE"
        requests.post(f"https://api.telegram.org/bot{self.bot_token}/sendMessage?chat_id={self.chat_id}&text={notification_text}")