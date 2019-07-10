import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from translate.settings.logging import Logger

from datetime import datetime, timedelta
import sendgrid
from sendgrid.helpers.mail import *

local_tz = pendulum.timezone("Asia/Taipei")

cron_expression = '0 */1 * * *'
update_execution_time = 9
remove_execution_time = 10

start_date = datetime(2019, 7, 3, 0, 0, tzinfo=local_tz)
default_args = {
    'owner': 'tim',
    'start_date': start_date,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'task_concurrency': 1
}

logger = Logger().get()


def notify_failure(context):
    personalization = Personalization()
    personalization.add_to(Email("t810308@gmail.com"))
    mail = Mail()
    mail.subject = context['ds'] + ' Hourly_report_failed.'
    mail.from_email = Email("data-source@gmail.com")
    mail.add_personalization(personalization)
    mail.add_content(Content("text/html", f'Hourly_report_failed FAILED'))
    sg = sendgrid.SendGridAPIClient(apikey=Variable.get('sendgrid_apikey'))
    response = sg.client.mail.send.post(request_body=mail.get())


def data_source_report(**context):
    execution_date = context['execution_date']
    start_hour = local_tz.convert(execution_date)
    from script.data_source_report import
    hbase_ads_report(start_hour)
    logger.info(f"{start_hour:%Y-%m-%dT%H} hbase_ads_report done")


with DAG('dag_hourly', default_args=default_args, schedule_interval=cron_expression) as dag:
    data_source_report = PythonOperator(
        task_id='data_source_report',
        provide_context=True,
        python_callable=data_source_report,
        on_failure_callback=notify_failure
    )

# data_source_report >> remove_text_task
