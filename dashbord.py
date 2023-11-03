import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
from pymongo import MongoClient

# Se connecter à MongoDB
client = MongoClient("mongodb://localhost:27017")
db_name = "users_profiles"
collection_name = "user_data"
db = client[db_name]
collection = db[collection_name]

# Récupérer les données de MongoDB et les stocker dans un DataFrame
data = list(collection.find({}))
df = pd.DataFrame(data)

# Initialisation de l'application Dash
app = dash.Dash(__name__)

# Création du layout du tableau de bord
app.layout = html.Div([
    html.H1("Tableau de bord de données utilisateur"),
    
    # Pie chart des 3 domaines les plus utilisés
    dcc.Graph(id='domain-pie-chart'),
    
    # Top 5 des pays avec le plus d'inscriptions
    dcc.Graph(id='top-countries-bar-chart'),
    
    # Diagramme circulaire du taux par genre
    dcc.Graph(id='gender-pie-chart'),
    
    # Username les plus anciens
    dcc.Graph(id='oldest-username-bar-chart')
])

@app.callback(
    Output('domain-pie-chart', 'figure'),
    Output('top-countries-bar-chart', 'figure'),
    Output('gender-pie-chart', 'figure'),
    Output('oldest-username-bar-chart', 'figure'),
    Input('domain-pie-chart', 'clickData')
)
def update_charts(clickData):
    # Pie chart des 3 domaines les plus utilisés
    domain_counts = df['domain_name'].value_counts().head(3)
    domain_pie_fig = px.pie(domain_counts, names=domain_counts.index, values=domain_counts.values, title='Domaines les plus utilisés')
    
    # Top 5 des pays avec le plus d'inscriptions
    top_countries = df['country'].value_counts().head(5)
    top_countries_bar_fig = px.bar(top_countries, x=top_countries.index, y=top_countries.values, title='Top 5 des pays avec le plus d\'inscriptions')
    
    # Diagramme circulaire du taux par genre
    gender_counts = df['gender'].value_counts()
    gender_pie_fig = px.pie(gender_counts, names=gender_counts.index, values=gender_counts.values, title='Taux par genre')
    
    # Username les plus anciens
    df['inscription'] = pd.to_datetime(df['inscription'])
    oldest_users = df.sort_values('inscription').head(5)
    oldest_username_bar_fig = px.bar(oldest_users, x='username', y='inscription', title='Username les plus anciens')
    
    return domain_pie_fig, top_countries_bar_fig, gender_pie_fig, oldest_username_bar_fig


if __name__ == '__main__':
    app.run_server(debug=True)
