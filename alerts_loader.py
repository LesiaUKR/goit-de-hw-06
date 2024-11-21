import pandas as pd

def load_alert_conditions(filepath):
    """
    Завантажує умови алертів із CSV-файлу та динамічно зчитує назви колонок.
    """
    conditions = pd.read_csv(filepath)
    return conditions

if __name__ == "__main__":
    filepath = "alerts_conditions.csv"
    conditions = load_alert_conditions(filepath)
    print(conditions.head())

