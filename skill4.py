import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression

# -----------------------------
# EXTRACT
# -----------------------------
def extract_data(file_path):
    print("Extracting data...")
    # Ensure you have a 'sales.csv' in the same folder or change this path
    data = pd.read_csv(file_path)
    return data

# -----------------------------
# TRANSFORM
# -----------------------------
def transform_data(data):
    print("Transforming data...")
    X = data[['advertising']]
    y = data['sales']
    return X, y

# -----------------------------
# LOAD
# -----------------------------
def load_model(X, y):
    print("Loading data into AI model...")
    model = LinearRegression()
    model.fit(X, y)
    return model

# -----------------------------
# VISUALIZATION
# -----------------------------
def plot_results(data, model):
    print("Plotting results...")
    plt.figure(figsize=(10, 6))
    
    # Plotting actual data points
    plt.scatter(data['advertising'], data['sales'], color='blue', label='Actual Data')
    
    # Plotting the regression line
    X_range = pd.DataFrame({'advertising': [data['advertising'].min(), data['advertising'].max()]})
    y_pred = model.predict(X_range)
    plt.plot(X_range, y_pred, color='red', linewidth=2, label='Regression Line')
    
    plt.xlabel("Advertising Spend")
    plt.ylabel("Sales")
    plt.title("Advertising vs Sales (Linear Regression)")
    plt.legend()
    plt.grid(True)
    
    # This command opens the window in VS Code
    plt.show()

# -----------------------------
# PIPELINE EXECUTION
# -----------------------------
if __name__ == "__main__":
    # Note: Make sure sales.csv exists in your directory!
    try:
        raw_data = extract_data("sales.csv")
        X, y = transform_data(raw_data)
        trained_model = load_model(X, y)

        # Updated visualization call
        plot_results(raw_data, trained_model)

        # Inference
        new_data = pd.DataFrame({'advertising': [150]})
        prediction = trained_model.predict(new_data)
        print(f"Predicted Sales for 150 advertising spend: {prediction[0]:.2f}")
        
    except FileNotFoundError:
        print("Error: 'sales.csv' not found. Please ensure the file is in the correct folder.")