import requests
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt


def get_currency(currencies_dict):
    if currencies_dict:
        return list(currencies_dict.keys())[0]
    return None


def pobierz_dane():
    url = "https://restcountries.com/v3.1/all?fields=name,capital,region,subregion,population,area,currencies"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def utworz_dataframe(data):
    kraje = []

    for kraj in data:
        capital = kraj.get("capital")

        kraje.append({
            "nazwa": kraj.get("name", {}).get("common"),
            "stolica": capital[0] if capital else None,
            "region": kraj.get("region"),
            "subregion": kraj.get("subregion"),
            "populacja": kraj.get("population"),
            "powierzchnia": kraj.get("area"),
            "waluta": get_currency(kraj.get("currencies"))
        })

    return pd.DataFrame(kraje)


def zapisz_do_bazy(df):
    conn = sqlite3.connect("kraje_swiata.db")
    df.to_sql("kraje", conn, if_exists="replace", index=False)
    return conn


def wykonaj_analizy(conn):

    print("\nŁączna populacja świata:")
    print(pd.read_sql_query("""
        SELECT SUM(populacja) AS laczna_populacja
        FROM kraje
    """, conn))

    print("\n10 krajów z największą populacją:")
    print(pd.read_sql_query("""
        SELECT nazwa, populacja
        FROM kraje
        ORDER BY populacja DESC
        LIMIT 10
    """, conn))

    print("\nLiczba krajów i średnia populacja w każdym regionie:")
    print(pd.read_sql_query("""
        SELECT region,
               COUNT(*) AS liczba_krajow,
               AVG(populacja) AS srednia_populacja
        FROM kraje
        GROUP BY region
        ORDER BY liczba_krajow DESC
    """, conn))

    print("\nKraje większe od Polski:")
    print(pd.read_sql_query("""
        SELECT nazwa, powierzchnia
        FROM kraje
        WHERE powierzchnia > 312679
        ORDER BY powierzchnia DESC
    """, conn))

    print("\nKraj z najwyższą gęstością zaludnienia:")
    print(pd.read_sql_query("""
        SELECT nazwa,
               populacja,
               powierzchnia,
               populacja / powierzchnia AS gestosc_zaludnienia
        FROM kraje
        WHERE powierzchnia > 0
        ORDER BY gestosc_zaludnienia DESC
        LIMIT 1
    """, conn))


def utworz_wykres(conn):

    df_regiony = pd.read_sql_query("""
        SELECT region, SUM(populacja) AS laczna_populacja
        FROM kraje
        GROUP BY region
        ORDER BY laczna_populacja DESC
    """, conn)

    df_regiony.plot(
        kind="bar",
        x="region",
        y="laczna_populacja",
        legend=False
    )

    plt.title("Łączna populacja według regionów")
    plt.xlabel("Region")
    plt.ylabel("Populacja")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


def main():
    data = pobierz_dane()
    df = utworz_dataframe(data)

    print("Pierwsze 5 wierszy DataFrame:")
    print(df.head())

    print("\nRozmiar DataFrame:")
    print(df.shape)

    print("\nTypy danych:")
    print(df.dtypes)

    conn = zapisz_do_bazy(df)

    wykonaj_analizy(conn)
    utworz_wykres(conn)

    conn.close()


main()
