import yfinance as yf
import sqlite3
import pandas as pd
import numpy as np

def liquidity(data):
    df = data.copy()

    required_cols = ["TotalAssets", "Inventory", "TotalDebt"]
    if not all(col in df.columns for col in required_cols):
        print(f"Faltan columnas requeridas: {', '.join([col for col in required_cols if col not in df.columns])}")
        return df

    # Calcular liquidez corriente
    df["Liquidity"] = df["TotalAssets"] / df["TotalDebt"]

    # Calcular liquidez ácida
    df["Acid Liquidity"] = (df["TotalAssets"] - df["Inventory"]) / df["TotalDebt"]

    return df

def get_balancesheets(types,tickers):
    ruta_balances = sqlite3.connect("C:/Users/thisi/OneDrive/Desktop/Python Crash Course/Proyectos/Automatic Balance Analysis/Bases de datos/balances.db")

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
                    combinado = balance
                    combinado = liquidity(combinado)

                combinado.to_sql(tabla_nombre, ruta_balances, if_exists="replace", index=False)
                print(f"Balance actualizado para {ticker}.")
            else:
                print(f"No se pudo obtener el balance para {ticker}.")

    ruta_balances.close()

tickers = [
   'ALUA.BA','BBAR.BA','BMA.BA','BYMA.BA','CEPU.BA',
   'COME.BA','CRES.BA','EDN.BA','GGAL.BA','IRSA.BA',
   'LOMA.BA','METR.BA','MIRG.BA','PAMP.BA','SUPV.BA',
   'SUPV.BA','TECO2.BA','TGNO4.BA','TGSU2.BA','TRAN.BA',
   'TXAR.BA','VALO.BA','YPFD.BA'
]

types = ["yearly","quarterly"]

get_balancesheets(types,tickers)