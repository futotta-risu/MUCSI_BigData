from calendar import c
import luigi
import luigi.contrib.postgres
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
from sqlalchemy import create_engine

class SensorDataIngestion(luigi.Task):
    def output(self):
        return luigi.LocalTarget('preprocessing/AirQualityUCI.csv')

class SensorDataCleaning(luigi.Task):
    def requires(self):
        return SensorDataIngestion()
    def output(self):
        return luigi.LocalTarget('AirQualityUCI_clean.csv')
    def run(self):
        df = pd.read_csv(self.requires().output().path, delimiter=';')
        
        df.Date = pd.to_datetime(df['Date'] + df['Time'], format='%d/%m/%Y%H.%M.%S')
        df.drop(['Time'], axis=1, inplace=True)
        
        df.dropna(how='all', inplace=True)
        df.dropna(how='all', axis=1, inplace=True)
        df.replace(-200, np.NaN, inplace=True)
        df.replace("-200", np.NaN, inplace=True)
        
        df.replace(",", ".", regex=True, inplace=True)
        for column in df.columns:
            if column != 'Date': df[column] = df[column].astype(np.float64)

        with open(self.output().path, 'wb') as output_file:
            df.to_csv(output_file, index=False)
        

class ErrorMeasuresReport(luigi.Task):
    def requires(self):
        return SensorDataCleaning()
    def output(self):
        return luigi.LocalTarget('nullCount.csv')
    def run(self):
        df = pd.read_csv(self.requires().output().path, delimiter=',')
        null_count_df = pd.DataFrame()
        for i, null_count in enumerate(df.isnull().sum()):
            null_count_df = pd.concat([null_count_df, pd.Series([df.columns[i], np.around(null_count/df.shape[0]*100, 2)]).to_frame().T], ignore_index=True)
        
        null_count_df = null_count_df.set_axis(['Variables', 'NullPercent'], axis=1) 
        with open(self.output().path, 'wb') as output_file:
            null_count_df.to_csv(output_file, index=False, header=True)

class DailyAverageReport(luigi.Task):
    initDate = luigi.DateParameter(default=datetime(2004, 3, 10))
    endDate = luigi.DateParameter(default=datetime(2005, 4, 5))

    def requires(self):
        return SensorDataCleaning()
    def output(self):
        return luigi.LocalTarget('dailyAggregate.csv')
    def run(self):
        df = pd.read_csv(self.requires().output().path, delimiter=',')
        df.Date = df.Date.astype(np.datetime64)
        for column in df.columns:
            if column != 'Date': df[column] = df[column].astype(np.float64)

        averageDf = pd.DataFrame(columns=df.columns)

        for i, day in enumerate(np.arange(self.initDate, self.endDate, timedelta(days=1)).astype(datetime)):

                nextDay = day + timedelta(days=1)
                dfSplitedDay = df[(df['Date'] >= day) & (df['Date'] < nextDay)]
                dfSplitedDay = dfSplitedDay.replace(np.NaN, 0)
                
                tempSeries = pd.Series(day, index=['Date'])
                if len(dfSplitedDay.index) > 0:
                    tempSeries = pd.concat([np.around(dfSplitedDay.describe().mean(), 2), tempSeries])
                    averageDf = pd.concat([averageDf, tempSeries.to_frame().T], ignore_index=True)
                
        with open(self.output().path, 'wb') as output_file:
            averageDf.to_csv(output_file, index=False)


class LoadErrorMeasures(luigi.Task):
    result = -1
    def requires(self):
        return ErrorMeasuresReport()
    def complete(self):
        return True if self.result >0 else False 
    #def output(self):      
        #return(luigi.contrib.postgres.PostgresTarget('localhost','sensors','postgres','12345','error_measures', '1','5432'))
    def run(self):
        df = pd.read_csv(self.requires().output().path, delimiter=',')
        engine = create_engine('postgresql://postgres:12345@localhost:5432/sensors')
        self.result = df.to_sql(
            'error_measures',
            engine,
            index=True,
            if_exists='replace'
            )
        
class LoadDailyAverages(luigi.Task):
    initDate = luigi.DateParameter(default=datetime(2004, 3, 10))
    endDate = luigi.DateParameter(default=datetime(2005, 4, 5))
    result = -1
    def requires(self):
        return DailyAverageReport(self.initDate, self.endDate)
    def complete(self):
        return True if self.result >0 else False 
    #def output(self):      
        #return(luigi.contrib.postgres.PostgresTarget('localhost','sensors','postgres','12345','error_measures', '1','5432'))
    def run(self):
        df = pd.read_csv(self.requires().output().path, delimiter=',')
        engine = create_engine('postgresql://postgres:12345@localhost:5432/sensors')
        self.result = df.to_sql(
            'daily_averages',
            engine,
            index=False,
            if_exists='replace'
            )

class LoadAllReports(luigi.WrapperTask):
    initDate = luigi.DateParameter(default=datetime(2004, 3, 10))
    endDate = luigi.DateParameter(default=datetime(2005, 4, 5))
    
    def requires(self):
        yield LoadErrorMeasures()
        yield LoadDailyAverages(self.initDate, self.endDate)

if __name__ == '__main__':
    luigi.build([LoadAllReports(initDate=datetime(2004,3,10), endDate=datetime(2005, 4, 5))], workers=1, local_scheduler=True)



