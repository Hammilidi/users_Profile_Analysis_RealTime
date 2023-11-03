import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pymongo
import pandas as pd

# Connexion à MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["users_profiles"]
collection = db["user_data"]

# Créer une application Dash
app = dash.Dash(__name__)

# Définir la mise en page du tableau de bord
app.layout = html.Div([
    html.H1("Tableau de bord des données utilisateur"),
    dcc.Graph(id='user-data-graph'),
])

# Fonction pour extraire des données de MongoDB
def get_data_from_mongodb():
    data = list(collection.find())
    df = pd.DataFrame(data)
    return df

# Mise à jour du tableau de bord en fonction des données MongoDB
@app.callback(
    Output('user-data-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_graph(n):
    df = get_data_from_mongodb()
    # Vous pouvez utiliser Plotly pour créer des visualisations interactives ici
    # Exemple : Créez un graphique à barres, un graphique à secteurs, un nuage de points, etc.
    # En fonction de vos besoins.

if __name__ == '__main__':
    app.run_server(debug=True)
