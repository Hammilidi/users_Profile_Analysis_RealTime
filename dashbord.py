import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from pymongo import MongoClient
import pandas as pd

# Connectez-vous à la base de données MongoDB
client = MongoClient("mongodb://localhost:27017")
db_name = "userdbd"
collection_name = "userco11"
db = client[db_name]
collection = db[collection_name]

# Récupérez les données de MongoDB et stockez-les dans un DataFrame
data = list(collection.find())
df = pd.DataFrame(data)

# Initialisez l'application Dash
app = dash.Dash(__name__)

# Définissez la mise en page du tableau de bord
app.layout = html.Div([
    dcc.Graph(id='age-distribution'),
    dcc.Dropdown(
        id='gender-dropdown',
        options=[
            {'label': 'Male', 'value': 'male'},
            {'label': 'Female', 'value': 'female'}
        ],
        value='male'
    )
])

# Créez une fonction de mise à jour pour le graphique en fonction de la sélection de genre
@app.callback(
    Output('age-distribution', 'figure'),
    Input('gender-dropdown', 'value')
)
def update_graph(selected_gender):
    filtered_df = df[df['gender'] == selected_gender]
    age_distribution = filtered_df['age'].value_counts().sort_index()
    figure = {
        'data': [
            {'x': age_distribution.index, 'y': age_distribution.values, 'type': 'bar', 'name': 'Age Distribution'},
        ],
        'layout': {
            'title': f'Age Distribution for {selected_gender} Users',
            'xaxis': {'title': 'Age'},
            'yaxis': {'title': 'Count'}
        }
    }
    return figure

if __name__ == '__main__':
    app.run_server(debug=True)
