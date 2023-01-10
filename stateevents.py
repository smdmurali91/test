import logging
import os
from datetime import datetime, timedelta
import pymssql
from timeloop.app import Timeloop

from gabor_agent_sdk.healthchecks import HealthCheckItem, RobustBackupHealthCheck
from gabor_agent_sdk.publishers import BaseStateEventFactory

LOG = logging.getLogger(__name__)
# Set up DB connect session 
bglServer = os.getenv('bglServer')
bglPort = os.getenv('bglPort')
bglDatabase = os.getenv('bglDatabase')
bglusername = os.getenv('bglusername')
bglpassword = os.getenv('bglpassword')

nzServer = os.getenv('nzServer')
nzPort = os.getenv('nzPort')
nzDatabase = os.getenv('nzDatabase')
nzusername = os.getenv('nzusername')
nzpassword = os.getenv('nzpassword')

# Open and read the file as a single buffer
fd = open('health_monitor.sql', 'r')
sql_query = fd.read()
fd.close()

rep = open('replication_monitor.sql', 'r')
rep_sql_query = rep.read()
rep.close()


class SentryRobustBackupHealthCheck(RobustBackupHealthCheck):
    def __init__(self, config: dict):
        super().__init__()
        self.register_health_check_item(HealthCheckItem('Latency', True))
        self.register_health_check_item(HealthCheckItem('Replication active', True))
        self.tl = tl = Timeloop()

        @tl.job(interval=timedelta(seconds=config['agent.latency.check.interval.sec']))
        def check_latency():

            try:
                bglconn = pymssql.connect(host=bglServer, port=bglPort, user=bglusername, password=bglpassword,
                                          database='distdb')
                bglcursor = bglconn.cursor()
                bglcursor.execute(rep_sql_query)
                replication_timestamp = bglcursor.fetchone()[3]
                LOG.info(f'[REPLICATION_LATENCY] The Replication Latency is {replication_timestamp}')
                latency_ok = True if datetime.now() - replication_timestamp < timedelta(minutes=15) else False
                bglconn.close()
            except Exception as e:
                LOG.error("Not able to connect to BGL DB")
                LOG.exception(e)
                latency_ok = False
            self.set_health_check('Latency', latency_ok)

        @tl.job(interval=timedelta(seconds=config['agent.replication.check.interval.sec']))
        def check_replication():

            try:
                bglconn = pymssql.connect(host=bglServer, port=bglPort, user=bglusername, password=bglpassword,
                                          database='distdb')
                bglcursor = bglconn.cursor()
                # Prepare the stored procedure execution script
                storedProc = "Exec [dbo].[GetReplicationAgentStatus]"
 
                # Execute Stored Procedure
                bglcursor.execute(storedProc)
                
                replication_status = bglcursor.fetchone()[1]
                LOG.info(f'[REPLICATION_STATUS] The Replication check is {replication_status}')
                replication_active = True if replication_status == 'Running' else False
                bglconn.close()
            except Exception as e:
                LOG.error("Not able to connect to BGL DB")
                LOG.exception(e)
                replication_active = False
            self.set_health_check('Replication active', replication_active)

        @tl.job(interval=timedelta(seconds=config['agent.asset.check.interval.sec']))
        def check_asset():
            asset_ok = False
            try:
                bglconn = pymssql.connect(host=bglServer, port=bglPort, user=bglusername, password=bglpassword,
                                          database=bglDatabase)
                bglcursor = bglconn.cursor()
                bglcursor.execute('SELECT VersionName from ' + bglDatabase + '.dbo.SchemaInfo')
                asset_ok = True
                bglconn.close()
            except Exception as e:
                LOG.error("Not able to connect to BGL DB")
                LOG.exception(e)
                asset_ok = False
            self.set_health_check('Asset health', asset_ok)

        self.tl.start()

    def destroy(self):
        self.tl.stop()


class SentryStateEventFactory(BaseStateEventFactory):
    def __init__(self, config: dict):
        super().__init__(SentryRobustBackupHealthCheck(config))
        self.replication_data = []
        self.tl = tl = Timeloop()

        @tl.job(interval=timedelta(seconds=config['agent.replication.data.refresh.interval.sec']))
        def refresh_replication_data():
            try:
                bglconn = pymssql.connect(host=bglServer, port=bglPort, user=bglusername, password=bglpassword,
                                          database=bglDatabase)
                bglcursor = bglconn.cursor()
                LOG.info(f'[REPLICATION_CHECK] Fetching replication data from the stored procedure')

                bglcursor.execute(sql_query)
                column_names = [col[0] for col in bglcursor.description]
                LOG.info(f'[REPLICATION_CHECK] Column names from the stored procedure are {column_names}')
                dataframe = []
                for row in bglcursor.fetchall():
                    dataframe.append({name: str(row[i]) for i, name in enumerate(column_names)})

                LOG.info(f'[REPLICATION_CHECK] data from the stored procedure are {dataframe}')

                self.replication_data = dataframe
                bglconn.close()
            except Exception as e:
                print("Not able to connect to bgl DB")
                print(e)

        self.tl.start()

    def create_event(self, config: dict, state: str) -> dict:
        event = {}
        event.update(super().create_event(config, state))
        event['replicationData'] = self.replication_data
        # Retrieve Sentry application version from [SchemaInfo] table
        try:
            bglconn = pymssql.connect(host=bglServer, port=bglPort, user=bglusername, password=bglpassword,
                                      database=bglDatabase)
            bglcursor = bglconn.cursor()
            bglcursor.execute('SELECT VersionName from ' + bglDatabase + '.dbo.SchemaInfo')
            sentryVersion = str(bglcursor.fetchone()[0])
            bglconn.close()
        except:
            print("Unable to retrieve Sentry version.")
            sentryVersion = 'unknown'
        event['additional'] = {'assetVersion': sentryVersion}
        return event

    def destroy(self):
        super().destroy()
        self.tl.stop()