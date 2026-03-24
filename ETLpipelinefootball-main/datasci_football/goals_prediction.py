import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
import joblib

df = pd.read_csv('top_scorers.csv')


features = ['matches', 'assists', 'average_rating', 'player_of_the_week', 'position']
X = df[features]
y = df['goals']

# onehot encode
X = pd.get_dummies(X, columns=['position'], drop_first=True)


X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# บันทึกโมเดล
joblib.dump(model, 'goals_prediction_model.pkl')


predictions = model.predict(X_test)


mae = mean_absolute_error(y_test, predictions)
r2 = r2_score(y_test, predictions)


print(f"MAE {mae:.2f} ประตู")
print(f"R**2:{r2*100:.2f}% ")


