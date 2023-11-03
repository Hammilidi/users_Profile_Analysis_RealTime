import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pymongo
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Connexion à MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["users_profiles"]
collection = db["user_data"]

# Créer une application Dash
app = dash.Dash(__name__)

# Définir la mise en page du tableau de bord
app.layout = html.Div([
    html.H1("Tableau de bord des données utilisateur"),
    
    dcc.Graph(id='user-data-subplot'),
    
    dcc.Interval(
        id='interval-component',
        interval=60*1000,
        n_intervals=0
    )
])

# Fonction pour extraire des données de MongoDB
def get_data_from_mongodb():
    data = list(collection.find())
    df = pd.DataFrame(data)
    return df

# Mise à jour du tableau de bord en fonction des données MongoDB
@app.callback(
    Output('user-data-subplot', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_graph(n):
    df = get_data_from_mongodb()
    
    # Calcul du nombre d'utilisateurs par genre
    num_users_by_gender = df['gender'].value_counts().reset_index()
    num_users_by_gender.columns = ['Gender', 'Count']
    gender_bar_fig = px.bar(num_users_by_gender, x='Gender', y='Count', labels={'Count': 'Nombre d\'utilisateurs'})
    
    # Calcul du top 10 des pays les plus nombreux
    top_10_countries = df['Country'].value_counts().head(10).reset_index()
    top_10_countries.columns = ['Country', 'Count']
    countries_bar_fig = px.bar(top_10_countries, x='Country', y='Count', labels={'Count': 'Nombre d\'utilisateurs'})
    
    # Calcul du top 5 des domain_names les plus utilisés
    top_5_domain_names = df['domain_name'].value_counts().head(5).reset_index()
    top_5_domain_names.columns = ['Domain', 'Count']
    domain_names_pie_fig = px.pie(top_5_domain_names, values='Count', names='Domain', labels={'Count': 'Nombre d\'utilisateurs'})
    
    # Calcul de l'âge moyen des utilisateurs
    average_age = df['age'].mean()
    
    # Calcul du top 5 des utilisateurs les plus anciens
    top_5_oldest_users = df.sort_values(by='inscription', ascending=True).head(5)
    oldest_users_bar_fig = px.bar(top_5_oldest_users, x='full_name', y='inscription', labels={'inscription': 'Date d\'inscription'})
    
    # Création d'un subplot avec tous les graphiques
    fig = make_subplots(rows=3, cols=2, 
                        subplot_titles=['Nombre d\'utilisateurs par genre', 'Top 10 des pays les plus nombreux',
                                        'Top 5 des domain_names les plus utilisés', 'Âge moyen des utilisateurs',
                                        'Top 5 des utilisateurs les plus anciens'])
    
    fig.add_trace(gender_bar_fig.data[0], row=1, col=1)
    fig.add_trace(countries_bar_fig.data[0], row=1, col=2)
    fig.add_trace(domain_names_pie_fig.data[0], row=2, col=1)
    fig.add_trace(oldest_users_bar_fig.data[0], row=3, col=2)
    
    fig.update_layout(height=900, width=1200, title_text="Tableau de bord des données utilisateur")
    
    return fig

if __name__ == '__main__':
    app.run_server(debug=True)
