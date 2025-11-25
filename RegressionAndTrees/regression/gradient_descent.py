import pandas as pd 
import matplotlib.pyplot as plt



data = pd.read_csv('Hours and Scores.csv')

def loss_function(m, b, points):
    total_error = 0 
    for i in range(len(points)):
        x = points.iloc[i].Hours
        y = points.iloc[i].Scores
        total_error += (y - (m * x + b))** 2

    total_error = total_error / float(len(points))


def gradient_descent(m_now, b_now, points, learning_rate):
    m_gradient = 0
    b_gradient = 0 

    n = len(points)

    for i in range(n):
        x = points.iloc[i].Hours
        y = points.iloc[i].Scores

        m_gradient += - (2/n) * x * (y - (m_now * x + b_now))
        b_gradient += - (2/n) *(y - (m_now * x + b_now))

    m = m_now - learning_rate * m_gradient
    b = b_now - learning_rate * b_gradient

    return m, b

m = 0
b = 0
learning_rate = 1e-4
epochs = 1000

for i in range(1000):
    m, b = gradient_descent(m, b, data, learning_rate)

print(m, b)

plt.scatter(data.Hours, data.Scores, color="black")
plt.plot(list(range(0,10)), [m * x + b for x in range(0, 10)], color='red')
plt.grid()
plt.show()


