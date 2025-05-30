import yfinance as yf
import sqlite3
import pandas as pd
import time
import platform
import sys


def liquidity(data):
    df = data.copy()

    # Calcular liquidez corriente
    df["Liquidity"] = df["TotalAssets"] / df["TotalDebt"]

    # Calcular liquidez ácida
    df["Acid Liquidity"] = (df["TotalAssets"] - df["Inventory"]) / df["TotalDebt"]

    return df

def cap_solv(data):
    df = data.copy()

    # Calcular liquidez corriente
    df["Liquidity"] = df["TotalAssets"] / df["TotalDebt"]

    # Calcular liquidez ácida
    df["Acid Liquidity"] = (df["TotalAssets"] - df["Inventory"]) / df["TotalDebt"]

    return df

def roi(data):
    df = data.copy()

    # Calcular liquidez corriente
    df["Total Debt To Capital"] = df["CurrentLiabilities"] / df["StockholdersEquity"]

    # Calcular liquidez ácida
    df["Acid Liquidity"] = (df["TotalAssets"] - df["Inventory"]) / df["TotalDebt"]

    return df

def market_mesures(data):
    df = data.copy()

    # Calcular liquidez corriente
    df["Liquidity"] = df["TotalAssets"] / df["TotalDebt"]

    # Calcular liquidez ácida
    df["Acid Liquidity"] = (df["TotalAssets"] - df["Inventory"]) / df["TotalDebt"]

    return df

def info_obtained(types, tickers):
    #df = data.copy()
    for type in types:
        for ticker in tickers:
            activo = yf.Ticker(ticker)
            balance = activo.get_balance_sheet(freq=type)
            print(balance.T.columns)

def get_balancesheets2(types, tickers):
    ficha = []
    start_time = time.time()
    
    # Info del entorno
    ficha.append("ICHA TÉCNICA DE EJECUCIÓN")
    ficha.append(f"Fecha de ejecución: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    ficha.append(f"Sistema operativo: {platform.system()} {platform.release()}")
    ficha.append(f"Python versión: {platform.python_version()}")
    ficha.append(f"Pandas versión: {pd.__version__}")
    ficha.append(f"yfinance versión: {yf.__version__}")
    ficha.append("")

    ruta_balances = sqlite3.connect("C:/Users/thisi/OneDrive/Desktop/Python Crash Course/Proyectos/Automatic Balance Analysis/Bases de datos/balances.db")

    total_ok, total_fail, total_skipped = 0, 0, 0

    for type in types:
        for ticker in tickers:
            t = ticker.removesuffix(".BA")
            tabla_nombre = f"balance_{type}_{t}"

            try:
                balance_guardado = pd.read_sql_query(f"SELECT * FROM {tabla_nombre}", ruta_balances)
                print(f"\nBalance previamente guardado para {ticker}:\n", balance_guardado)
            except Exception as e:
                balance_guardado = pd.DataFrame()
                print(f"\nNo se encontró balance previo para {ticker}: {e}")

            try:
                activo = yf.Ticker(ticker)
                balance = activo.get_balance_sheet(freq=type)

                if balance is not None and not balance.empty:
                    balance = balance.T  # Transponer: fechas como filas
                    balance.reset_index(inplace=True)
                    balance.rename(columns={"index": "Date"}, inplace=True)
                    balance["Ticker"] = ticker

                    # Une balances si hay datos nuevos
                    if not balance_guardado.empty:
                        balance["Date"] = pd.to_datetime(balance["Date"])
                        balance_guardado["Date"] = pd.to_datetime(balance_guardado["Date"])

                        combinado = pd.concat([balance_guardado, balance], ignore_index=True)
                        combinado.drop_duplicates(subset=["Date"], keep="last", inplace=True)
                        combinado = liquidity(combinado)
                    else:
                        combinado = liquidity(balance)

                    combinado.to_sql(tabla_nombre, ruta_balances, if_exists="replace", index=False)
                    print(f"Balance actualizado para {ticker}.")
                    total_ok += 1
                else:
                    print(f"No se pudo obtener el balance para {ticker}.")
                    total_skipped += 1

            except Exception as e:
                print(f"Error al procesar {ticker}: {e}")
                total_fail += 1

    ruta_balances.close()

    # Tiempo total
    end_time = time.time()
    duration = round(end_time - start_time, 2)
    ficha.append("Tiempo total de ejecución: {:.2f} segundos".format(duration))
    ficha.append(f"Balances actualizados: {total_ok}")
    ficha.append(f"Balances omitidos (vacíos): {total_skipped}")
    ficha.append(f"Errores: {total_fail}")

    # Guardar ficha
    with open("ficha_tecnica_analisis_fundamental.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(ficha))

    print("\n✅ Ficha técnica generada: ficha_tecnica_balances.txt")

# Ejecutar
tickers = [
   'ALUA.BA','BBAR.BA','BMA.BA','BYMA.BA','CEPU.BA',
   'COME.BA','CRES.BA','EDN.BA','GGAL.BA','IRSA.BA',
   'LOMA.BA','METR.BA','MIRG.BA','PAMP.BA','SUPV.BA',
   'TECO2.BA','TGNO4.BA','TGSU2.BA','TRAN.BA','TXAR.BA',
   'VALO.BA','YPFD.BA'
]

types = ["yearly","quarterly"]

get_balancesheets2(types, tickers)
