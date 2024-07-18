from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import joblib
from model_load import load_and_prepare_data


def train_and_save_model(X_train, y_train, X_test, y_test, model_path, scaler_path):
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Предсказание на тестовой выборке
    y_pred = model.predict(X_test)

    # Оценка модели
    mse = mean_squared_error(y_test, y_pred)

    # Сохранение модели
    joblib.dump(model, model_path)
    joblib.dump(scaler, scaler_path)


if __name__ == "__main__":

    file_path = '/tmp/postgres_data.csv'
    X_train, X_test, y_train, y_test, scaler = load_and_prepare_data(file_path)
    model_path = '/temp/model.pkl'
    scaler_path = '/temp/scaler.pkl'
    train_and_save_model(X_train, y_train, X_test,
                         y_test, model_path, scaler_path)
