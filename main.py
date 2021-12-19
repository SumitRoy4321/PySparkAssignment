import csv
import os
import time

import pyspark
from flask import Flask, render_template
import requests, pandas
# import json, csv, tablib
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import coalesce

app = Flask(__name__)
# sc = SparkContext(appName="Simple App")
session = SparkSession.builder.appName('app').getOrCreate()


@app.before_first_request
def before_first_request():
    countries = ["India", "USA", "Pakistan", "China", "Sri Lanka", "Canada", "Germany", "South Korea", "Indonesia",
                 "Japan"]

    url = "https://covid-19-data.p.rapidapi.com/country"
    datafile = open('datacsv.csv', 'w')
    csv_writer = csv.writer(datafile)
    header = ["country", "code", "confirmed", "recovered", "critical", "deaths", "latitude", "longitude", "lastChange",
              "lastUpdate"]
    csv_writer.writerow(header)
    for country in countries:
        time.sleep(5)
        querystring = {"name": country}
        headers = {
            'x-rapidapi-host': "covid-19-data.p.rapidapi.com",
            'x-rapidapi-key': "f05f7d42c0msh5dd8e0b3890a836p17a3d0jsnc81f300765e1"
        }
        response = requests.request("GET", url, headers=headers, params=querystring)
        json_response = response.json()
        # csv_writer.writerow(json_response)
        values = []
        for key, value in json_response[0].items():
            print(key, ":", value)
            values.append(value)
        csv_writer.writerow(values)


@app.route('/')
def index():
    data = pandas.read_csv('datacsv.csv', header=0)
    my_data = data.values
    return render_template('home.html', myData=my_data)
    # return 'hello'


@app.route('/mostAffectedCountryByCovid')
def most_affected_country():
    df = session.read.csv('datacsv.csv', inferSchema=True, header=True)
    df.show()
    df = df.withColumn('Effected_Ratio', coalesce(F.col('deaths'), F.lit(0)) / coalesce(F.col('confirmed'), F.lit(1)))
    df.sort(F.col('Effected_Ratio').desc()).show()
    df.createOrReplaceTempView("COV")
    result = session.sql("""
        SELECT country, Effected_Ratio FROM COV WHERE Effected_Ratio=(SELECT MAX(Effected_Ratio) FROM COV)
        """).collect()
    return 'most affected country : {} with death ratio as {}'.format(result[0][0], result[0][1])


@app.route('/leastAffectedCountryByCovid')
def least_affected_country():
    df = session.read.csv('datacsv.csv', inferSchema=True, header=True)
    df.show()
    df = df.withColumn('Effected_Ratio', coalesce(F.col('deaths'), F.lit(0)) / coalesce(F.col('confirmed'), F.lit(1)))
    df.sort(F.col('Effected_Ratio').asc()).show()
    df.createOrReplaceTempView("COV")
    result = session.sql("""
        SELECT country, Effected_Ratio FROM COV WHERE Effected_Ratio=(SELECT MIN(Effected_Ratio) FROM COV)
        """).collect()
    return 'most affected country : {} with death ratio as {}'.format(result[0][0], result[0][1])


@app.route('/countryWithHighestCovidCases')
def highest_covid_cases():
    df = session.read.csv('datacsv.csv', inferSchema=True, header=True)
    df.show()
    df.createOrReplaceTempView("COV")
    result = session.sql("""
        SELECT country, confirmed FROM COV WHERE confirmed=(SELECT MAX(confirmed) FROM COV)
        """).collect()
    return 'country with highest covid cases of {} is {}'.format(result[0][1], result[0][0])


@app.route('/countryWithLeastCovidCases')
def least_covid_cases():
    df = session.read.csv('datacsv.csv', inferSchema=True, header=True)
    df.show()
    df.createOrReplaceTempView("COV")
    result = session.sql("""
        SELECT country, confirmed FROM COV WHERE confirmed=(SELECT MIN(confirmed) FROM COV)
        """).collect()
    return 'country with least covid cases of {} is {}'.format(result[0][1], result[0][0])


@app.route('/countryMostEfficientAgainstCovid')
def most_efficient_country():
    df = session.read.csv('datacsv.csv', inferSchema=True, header=True)
    df.show()
    df = df.withColumn('Efficiency', coalesce(F.col('recovered'), F.lit(0)) / coalesce(F.col('confirmed'), F.lit(1)))
    df.sort(F.col('Efficiency').asc()).show()
    df.createOrReplaceTempView("COV")
    result = session.sql("""
        SELECT country, Efficiency FROM COV WHERE Efficiency=(SELECT MAX(Efficiency) FROM COV)
        """).collect()
    return 'country fought against covid most efficiently is {} with total {} recovery rate'.format(
        result[0][0],
        result[0][1])


@app.route('/countryLeastEfficientAgainstCovid')
def least_efficient_country():
    df = session.read.csv('datacsv.csv', inferSchema=True, header=True)
    df.show()
    df = df.withColumn('Efficiency', coalesce(F.col('recovered'), F.lit(0)) / coalesce(F.col('confirmed'), F.lit(1)))
    df.sort(F.col('Efficiency').asc()).show()
    df.createOrReplaceTempView("COV")
    result = session.sql("""
        SELECT country, Efficiency FROM COV WHERE Efficiency=(SELECT MIN(Efficiency) FROM COV)
        """).collect()
    return 'country fought against covid least efficiently is {} with total {} recovery rate'.format(
        result[0][0],
        result[0][1])


@app.route('/countryStillSuffering')
def highest_critical_covid_cases():
    df = session.read.csv('datacsv.csv', inferSchema=True, header=True)
    df.show()
    df.createOrReplaceTempView("COV")
    result = session.sql("""
        SELECT country, critical FROM COV WHERE critical=(SELECT MAX(critical) FROM COV)
        """).collect()
    return 'country still suffering by covid is {} with highest critical cases {}'.format(result[0][0], result[0][1])


@app.route('/countryCovidFree')
def least_critical_covid_cases():
    df = session.read.csv('datacsv.csv', inferSchema=True, header=True)
    df.show()
    df.createOrReplaceTempView("COV")
    result = session.sql("""
        SELECT country, critical FROM COV WHERE critical=(SELECT MIN(critical) FROM COV)
        """).collect()
    return 'country almost free by covid is {} with least critical cases {}'.format(result[0][0], result[0][1])


@app.route('/totalcases')
def total_cases():
    df = session.read.csv('datacsv.csv', inferSchema=True, header=True)
    df.show()
    total = df.groupBy().agg(F.sum("confirmed")).collect()
    print("total cases : ", total)
    return 'total cases among the list :{}'.format(total[0][0])


if __name__ == '__main__':
    app.run(debug=True)
