import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from databricks import sql
from typing import Tuple, List, Optional
import os

# --- FLEXIBLE SECRET MANAGEMENT ---
# This block allows the app to run on both Local (.env) and Cloud (st.secrets)
# without changing any code.
try:
    from dotenv import load_dotenv
    load_dotenv() # Loads .env file if it exists locally
except ImportError:
    pass # If python-dotenv isn't installed (e.g. in cloud), just skip it

# --- PAGE SETUP ---
st.set_page_config(
    page_title="Football AI Analyst",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- UNIVERSAL CONFIG LOADER ---
def get_config(key_name):
    """
    Tries to get the secret from Streamlit Cloud first.
    If not found, falls back to local Environment Variables (.env).
    """
    try:
        return st.secrets[key_name]
    except (FileNotFoundError, KeyError):
        return os.getenv(key_name)

# Load Secrets
SERVER_HOSTNAME = get_config("DATABRICKS_HOSTNAME")
HTTP_PATH = get_config("DATABRICKS_HTTP_PATH")
ACCESS_TOKEN = get_config("DATABRICKS_TOKEN")

# Security Check: Stop app if keys are missing
if not all([SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN]):
    st.error("🚨 Configuration Error: Missing Credentials.")
    st.info("""
    **How to fix:**
    1. **Local:** Create a `.env` file with DATABRICKS_HOSTNAME, HTTP_PATH, and TOKEN.
    2. **GitHub/Streamlit Cloud:** Add these to the 'Secrets' settings in your deployment dashboard.
    """)
    st.stop()

# --- CUSTOM CSS ---
st.markdown("""
    <style>
    .main { padding-top: 1rem; }
    .stMetric { 
        background-color: #262730; 
        border: 1px solid #4f4f4f; 
        padding: 10px; 
        border-radius: 5px; 
        color: white;
    }
    div[data-testid="stSidebar"] { background-color: #262730; }
    </style>
    """, unsafe_allow_html=True)

# --- INFRASTRUCTURE MANAGEMENT ---
def start_cluster() -> Tuple[bool, str]:
    """Sends a REST API command to wake up the SQL Warehouse."""
    try:
        cluster_id = HTTP_PATH.split('/')[-1]
    except IndexError:
        return False, "Invalid HTTP Path configuration."

    api_url = f"https://{SERVER_HOSTNAME}/api/2.0/clusters/start"
    payload = {"cluster_id": cluster_id}
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    
    try:
        response = requests.post(api_url, json=payload, headers=headers)
        if response.status_code == 200:
            return True, "Signal Sent! Cluster is starting up..."
        elif response.status_code == 400 and "unexpected state Running" in response.text:
            return True, "Cluster is already running!"
        elif response.status_code == 400 and "unexpected state Pending" in response.text:
            return True, "Cluster is already waking up. Please wait."
        else:
            return False, f"API Error: {response.text}"
    except Exception as e:
        return False, f"Connection Exception: {str(e)}"

# --- DATA ACCESS LAYER ---
def get_db_connection():
    return sql.connect(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        access_token=ACCESS_TOKEN
    )

@st.cache_data(ttl=3600)
def fetch_teams(league_name: str) -> List[str]:
    try:
        conn = get_db_connection()
        query = f"""
        SELECT DISTINCT Team 
        FROM delta.`/mnt/gold/team_features` 
        WHERE League = '{league_name}'
        ORDER BY Team ASC
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df['Team'].tolist()
    except Exception:
        return []

def fetch_analysis_data(league: str, home: str, away: str) -> pd.DataFrame:
    """
    Fetches raw match history. 
    Attempts to get Goals and Opponent to build the Scoreboard.
    """
    conn = get_db_connection()
    league_safe = league.replace("'", "''")
    home_safe = home.replace("'", "''")
    away_safe = away.replace("'", "''")
    
    # Try to fetch extended data (Goals, Opponent)
    query = f"""
    SELECT MatchDate, Team, Venue, Points, Opponent, Goals_For, Goals_Against
    FROM delta.`/mnt/gold/team_features`
    WHERE League = '{league_safe}' 
    AND trim(Team) IN ('{home_safe}', '{away_safe}')
    ORDER BY MatchDate ASC 
    """
    
    try:
        df = pd.read_sql(query, conn)
    except Exception:
        # Fallback if Goals/Opponent columns don't exist yet
        query_basic = f"""
        SELECT MatchDate, Team, Venue, Points
        FROM delta.`/mnt/gold/team_features`
        WHERE League = '{league_safe}' 
        AND trim(Team) IN ('{home_safe}', '{away_safe}')
        ORDER BY MatchDate ASC 
        """
        df = pd.read_sql(query_basic, conn)
        
    conn.close()
    return df

def process_3_column_form(df: pd.DataFrame, team_name: str) -> pd.DataFrame:
    """Calculates Form + Formats Score/Opponent for display."""
    team_df = df[df['Team'] == team_name].copy()
    team_df = team_df.sort_values("MatchDate")
    
    # 1. Clean Numeric Data
    team_df['Points'] = pd.to_numeric(team_df['Points'], errors='coerce').fillna(0)
    team_df['Venue'] = team_df['Venue'].astype(str).str.strip().str.title()
    
    # 2. Construct 'Score' Column (e.g., "2-1") if data exists
    if 'Goals_For' in team_df.columns and 'Goals_Against' in team_df.columns:
        team_df['Score'] = (
            team_df['Goals_For'].fillna(0).astype(int).astype(str) + "-" + 
            team_df['Goals_Against'].fillna(0).astype(int).astype(str)
        )
    else:
        team_df['Score'] = "N/A" 

    if 'Opponent' not in team_df.columns:
        team_df['Opponent'] = "N/A"

    # 3. Calculate Form Metrics (Rolling 5)
    team_df['Overall_Form'] = team_df['Points'].rolling(window=5, min_periods=1).sum()
    
    # Home Form
    home_games = team_df[team_df['Venue'] == 'Home'].copy()
    home_games['Home_Form'] = home_games['Points'].rolling(window=5, min_periods=1).sum()
    team_df.loc[home_games.index, 'Home_Form'] = home_games['Home_Form']
    
    # Away Form
    away_games = team_df[team_df['Venue'] == 'Away'].copy()
    away_games['Away_Form'] = away_games['Points'].rolling(window=5, min_periods=1).sum()
    team_df.loc[away_games.index, 'Away_Form'] = away_games['Away_Form']
    
    # Forward Fill to fill gaps
    team_df['Home_Form'] = team_df['Home_Form'].ffill()
    team_df['Away_Form'] = team_df['Away_Form'].ffill()
    
    # Return sorted by Newest First
    return team_df.sort_values("MatchDate", ascending=False)

# --- VISUALIZATION LAYER ---
def plot_comparison(home_name: str, home_score: float, away_name: str, away_score: float):
    """Generates a grouped bar chart."""
    fig = go.Figure(data=[
        go.Bar(name=home_name, x=[home_name], y=[home_score], marker_color='#00CC96'),
        go.Bar(name=away_name, x=[away_name], y=[away_score], marker_color='#AB63FA')
    ])
    fig.update_layout(
        title_text='Venue-Specific Form Strength',
        barmode='group',
        height=350,
        yaxis_title="Points (Last 5 at Venue)",
        template="plotly_dark",
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)'
    )
    return fig

# --- MAIN APPLICATION LOGIC ---
with st.sidebar:
    st.title("Control Panel")
    selected_league = st.selectbox("Choose Competition", ["EPL", "La_Liga", "Bundesliga", "Serie_A", "Ligue_1"])
    st.markdown("---")
    if st.button("Start Cluster"):
        with st.spinner("Contacting Azure..."):
            success, msg = start_cluster()
            if success: st.success(msg)
            else: st.error(msg)

st.title("European Football Predictor")
st.markdown(f"**Current League:** {selected_league}")
st.markdown("---")

team_list = fetch_teams(selected_league)

if not team_list:
    st.warning("Connection to Databricks is currently inactive.")
else:
    col1, col2, col3 = st.columns([2, 0.5, 2])
    with col1:
        home_team = st.selectbox("Select Host", team_list, index=0, key="home")
    with col2:
        st.markdown("<br><h2 style='text-align: center; color: #888;'>VS</h2>", unsafe_allow_html=True)
    with col3:
        away_team = st.selectbox("Select Visitor", team_list, index=1, key="away")
    st.markdown("###")
    
    if st.button("Run Prediction Model", type="primary", use_container_width=True):
        if home_team == away_team:
            st.error("A team cannot play against itself.")
        else:
            with st.spinner("Calculating..."):
                try:
                    df = fetch_analysis_data(selected_league, home_team, away_team)
                    if not df.empty:
                        # Process the data
                        df_home = process_3_column_form(df, home_team)
                        df_away = process_3_column_form(df, away_team)
                        
                        # Get Latest Form Scores (Handle NaNs)
                        h_form = df_home.iloc[0]['Home_Form'] if not pd.isna(df_home.iloc[0]['Home_Form']) else 0
                        a_form = df_away.iloc[0]['Away_Form'] if not pd.isna(df_away.iloc[0]['Away_Form']) else 0
                        
                        # --- DASHBOARD ---
                        st.divider()
                        k1, k2, k3 = st.columns(3)
                        k1.metric(f"{home_team} (Home Form)", f"{h_form:.0f}")
                        k2.metric(f"{away_team} (Away Form)", f"{a_form:.0f}")
                        
                        # Prediction
                        diff = h_form - a_form
                        if diff >= 3:
                            k3.metric("Prediction", f"{home_team} Win", delta="Strong")
                        elif diff <= -3:
                            k3.metric("Prediction", f"{away_team} Win", delta="Strong", delta_color="inverse")
                        else:
                            k3.metric("Prediction", "Draw", delta="Tight")
                            
                        # Chart
                        st.plotly_chart(plot_comparison(home_team, h_form, away_team, a_form), use_container_width=True)
                        
                        # Detailed History Table (LAST 10 GAMES)
                        with st.expander("View Detailed Form History (Last 10 Games)", expanded=True):
                            c1, c2 = st.columns(2)
                            
                            # Define the columns we want to see
                            # We check if Opponent/Score exists first to avoid errors if fallback mode is active
                            cols_to_show = ['MatchDate', 'Venue', 'Home_Form', 'Away_Form']
                            if 'Opponent' in df_home.columns: cols_to_show.insert(2, 'Opponent')
                            if 'Score' in df_home.columns: cols_to_show.insert(3, 'Score')

                            def display_history(container, team, data):
                                container.subheader(f"{team}")
                                container.dataframe(
                                    data[cols_to_show].head(10).style.format({
                                        "MatchDate": lambda t: t.strftime("%Y-%m-%d"),
                                        "Home_Form": "{:.0f}",
                                        "Away_Form": "{:.0f}",
                                    }),
                                    hide_index=True,
                                    use_container_width=True
                                )

                            display_history(c1, home_team, df_home)
                            display_history(c2, away_team, df_away)
                    else:
                        st.error("No data found.")
                except Exception as e:
                    st.error(f"Error: {e}")