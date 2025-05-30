import pandas as pd
import yfinance as yf
import datetime as dt
import numpy as np
import platform
import time

tickers = [
   'ALUA.BA','BBAR.BA','BMA.BA','BYMA.BA','CEPU.BA',
   'COME.BA','CRES.BA','EDN.BA','GGAL.BA','IRSA.BA',
   'LOMA.BA','METR.BA','MIRG.BA','PAMP.BA','SUPV.BA',
   'TECO2.BA','TGNO4.BA','TGSU2.BA','TRAN.BA','TXAR.BA',
   'VALO.BA','YPFD.BA'
]

start = dt.datetime.today() - dt.timedelta(60)
end = dt.datetime.today()

def ema(series, length):
    return series.ewm(span=length, adjust=False).mean()

def rsi(data, n):
    "Función para calcular el RSI"
    df = data.copy()
    
    # Calcular la diferencia de precios (cambio)
    change = df["Close"].diff()

    # Calcular ganancias y pérdidas
    df["gain"] = np.where(change > 0, change, 0)
    df["loss"] = np.where(change < 0, -change, 0)

    # Calcular las medias exponenciales de ganancias y pérdidas
    avgGain = df["gain"].ewm(span=n, min_periods=n).mean()
    avgLoss = df["loss"].ewm(span=n, min_periods=n).mean()

    # Evitar división por cero en RS
    rs = avgGain / avgLoss.replace(0, np.nan)

    # Calcular RSI
    df["RSI"] = 100 - (100 / (1 + rs))
    return df["RSI"]

def analisis_tecnico(ticker):
    ficha = []
    start_time = time.time()
    
    # Info del entorno
    ficha.append("ICHA TÉCNICA DE EJECUCIÓN")
    ficha.append(f"Fecha de ejecución: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    ficha.append(f"Sistema operativo: {platform.system()} {platform.release()}")
    ficha.append(f"Python versión: {platform.python_version()}")
    ficha.append(f"Pandas versión: {pd.__version__}")
    ficha.append(f"yfinance versión: {yf.__version__}")
    ficha.append("Ubicación de la base de datos: balances.db")
    ficha.append("")

    total_ok, total_fail = 0, 0

    for ticker in tickers:
        yfinance_information = yf.download(ticker, start=start, end=end, interval="1d", auto_adjust=True)

        if not yfinance_information.empty:
            data = pd.DataFrame(yfinance_information)

            data.columns = ['Close', 'High', 'Low', 'Open', 'Vol']
            data['Ticker'] = ticker
            data["date"] = data.index

            data.info()
            data["EMA50"] = ema(data["Close"], 50)
            data["RSI"] = rsi(data, 14)

            total_ok += 1
        else:
            total_fail += 1

     # Tiempo total
    end_time = time.time()
    duration = round(end_time - start_time, 2)
    ficha.append("Tiempo total de ejecución: {:.2f} segundos".format(duration))
    ficha.append(f"Balances actualizados: {total_ok}")
    ficha.append(f"Errores: {total_fail}")

    # Guardar ficha
    with open("ficha_tecnica_analisis_tecnico.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(ficha))

analisis_tecnico(tickers)