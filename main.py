from fastapi import FastAPI
import mysql.connector
import pandas as pd
import time

db= mysql.connector.connect(
    host='localhost',
    user='root',
    passwd='root',
    database='stockdata'
)

Mycursor=db.cursor()
app=FastAPI()

companies=list()

def timestamp(t_date):
    return int(time.mktime(time.strptime(t_date, '%Y-%m-%d')))




def store_into_Database(date,open,high,low,close,adj_close,vol,comp_name):
    query="INSERT INTO share_log (Date,Open,High,Low,Close,Adj_Close,Volume,comp_name) VALUES ("+'"'+str(date)+'"'+','+str(open)+','+str(high)+','+str(low)+','+str(close)+','+str(adj_close)+','+str(vol)+','+'"'+comp_name+'"'+");"
    print(query)
    Mycursor.execute(query)
    Mycursor.execute('commit')


@app.get("/")
def main():
        return JSONResponse(content={"result":"Server working properly"},status_code=400)
        

@app.get("/stock/all")
#Get all companies’ stock data for a particular day
def data_on_this_day(start_date):
    try:
        res=[]
        query="SELECT * FROM share_log where Date="+"'"+start_date+"';"
        Mycursor.execute(query)
        for x in :
            res.append(x)
        return JSONResponse(content={"result":res},status_code=200)
    except:
        return JSONResponse(content={"result":"Data fetching failed "},status_code=400)


@app.get("/stock/company")
#Get all stock data for a particular company for a particular day 
def company_data_on_this_day(company,start_date):
    try:
        query="SELECT * FROM share_log where Date='"+ start_date+"'"+ "and comp_name='"+company+"';"
        res=[]
        Mycursor.execute(query)
        for x in :
            res.append(x)
        return JSONResponse(content={"result":res},status_code=200)
    except:
        return JSONResponse(content={"result":"Data fetching failed "},status_code=400)

@app.get("/stock/allspan")
#Get all stock data for a particular company
def company_data_all(company):
    try:
        query="SELECT * FROM share_log where comp_name='"+str(company)+"';"
        res=[]
        Mycursor.execute(query)
        for x in :
            res.append(x)
        return JSONResponse(content={"result":res},status_code=200)
    except:
        return JSONResponse(content={"result":"Data fetching failed "},status_code=400)

@app.post("/stock/update")
#POST/Patch API to update stock data for a company by date
def update_data(company,start_date,op):
    try:
        query="UPDATE share_log SET="+str(op)+ " WHERE comp_name='" +company+ "' and Date='"+start_date+ "';"
        Mycursor.execute(query)
        Mycursor.execute('commit')
        return JSONResponse(content={"result":"Update sucess"},status_code=200)
    except:
        return JSONResponse(content={"result":"Data update failed"},status_code=400)

@app.on_event("startup")
#To Automatically upload stock data into the database on api startup 
def upload_data(start_date):
    f= open("config.txt",'r')
    for company_name in f:
        companies.append(company_name.rstrip())
    f.close()
    print(companies)
    for company in companies:
        url= f'https://query1.finance.yahoo.com/v7/finance/download/{company}?period1={timestamp(start_date)}&period2={int(time.time())}&interval=1d&events=history&includeAdjustedClose=true'      
        df=pd.read_csv(url)
        for rows in df.index:
            store_into_Database(df["Date"][rows],df["Open"][rows],df["High"][rows],df["Low"][rows],df["Close"][rows],df["Adj Close"][rows],df["Volume"][rows],company)
    return "Data Upload Success"


