from feast import FeatureStore
from sklearn.metrics import accuracy_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
import logging
import mlflow
import pickle


#create log file if it does not exist
modelling_log_file = "C:\\Annie\\Trial/logs/modelling.log"
logging.root.handlers = []
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO , filename=modelling_log_file)

# set up logging
console = logging.StreamHandler()
console.setLevel(logging.ERROR)

# set a format which is simpler for console use
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
console.setFormatter(formatter)
logging.getLogger("").addHandler(console)
def retrieve_data_from_feast():
    store = FeatureStore(repo_path="")
    features = store.get_historical_features(
        features=["customer_churn_features:CreditScore"],
        entity_df=None
    ).to_df()
    return features
  
def train_log_models():
    data = retrieve_data_from_feast();
    x = data.drop('Exited', axis=1)  # Features (all columns except 'Amount')
    y = data['Exited']  # Target variable ('Amount' column)
    x_train, x_test, y_train, y_test = train_test_split(
    x, y, test_size=0.2, random_state=42)  # 0.2 represents 20% test size
    
    models = { 
              "RandomForest" : RandomForestClassifier(n_estimators=100),
              "LogisticRegression" : LogisticRegression,
              "SVM" : SVC(probability=True),
              
              }
    
    best_model = None
    best_accuracy = 0
    best_run_id = None
    mlflow.set_tracking_uri("https://localhost:5000")
    for model_name, model in models.items():
        model.fit(x_train, y_train)
        y_pred = model.predict(x_test)
        accuracy = accuracy_score(y_test, y_pred)
        with mlflow.start_run() as run:
            mlflow.log_param("model_type", model_name)
            mlflow.log_metric("accuracy", accuracy)
            mlflow.sklear.log_model(model, f"{model_name}_model")
            with open(f"models/{model_name}.pkl", "wb") as f:
                pickle.dump(model,f)
            if accuracy > best_accuracy:
                best_accuracy = accuracy
                best_model = model_name
                best_run_id = run.info.run_id
    with open("models/best_model.txt", "w") as f:
        f.write(f"Best Run ID: {best_run_id}, Best Model: {best_model}, Best Accuracy:{best_accuracy}")
        
    return (best_model, best_run_id, best_accuracy)
    
def register_best_model():
    best_model, best_run_id, best_accuracy = train_log_models()
    model_uri = f"runs:/{best_run_id}/{best_model}_model"
    mlflow.register_model(model_uri, "Best_ML_Model")
    
try:    
    register_best_model()
except Exception as e:
    print(str(e))




