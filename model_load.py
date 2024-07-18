import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler


def load_and_prepare_data(file_path):

    df = pd.read_csv(file_path)

    df['flight_hour'] = pd.to_datetime(df['flight_hour'])
    df['weekday'] = df['flight_hour'].dt.weekday
    df['month'] = df['flight_hour'].dt.month

    df = df.dropna(inplace=True)

    X = df[['weekday', 'month', 'flight_duration',
            'flight_distance', 'num_passengers', 'weather']]
    y = df['demand']

    # обучающая и тестовая выборка
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42)

    # Нормализация
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    return X_train_scaled, X_test_scaled, y_train, y_test, scaler
