import matplotlib.pyplot as plt
from sklearn import datasets, linear_model
from sklearn.model_selection import train_test_split
import numpy as np
from sklearn.metrics import mean_squared_error, r2_score

diabeties = datasets.load_diabetes() # could be x,y = datasets.load_diabetes(return_X_y=True) 

x = diabeties.data # type: ignore
y = diabeties.target # type: ignore

x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2) # puts 20 percent to test and 80 percent to train

model = linear_model.LinearRegression()

model.fit(x_train, y_train)
y_pred = model.predict(x_test)

print(model.coef_, model.intercept_, mean_squared_error(y_test, y_pred), r2_score(y_test, y_pred))

plt.scatter(y_test,y_pred, alpha=0.5)
plt.show()

person = np.mean(x, axis=0)

# In the dataset, 'age' is at index 0, 'sex' at index 1
person[0] = 0.03   # slightly above average age (approx. 40 years)
person[1] = -0.05  # female (negative in the normalized sex encoding)

# Predict
prediction = model.predict([person]) # type: ignore
print(prediction)

















