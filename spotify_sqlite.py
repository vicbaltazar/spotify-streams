import sqlite3
import pandas as pd

# 1) Ler o arquivo CSV
csv_path = "Streaming_History_Audio_2025-2026_2.csv"

df = pd.read_csv(csv_path)

print("Colunas do arquivo:", df.columns.tolist())
print("Qrd linhas:", len(df))

# 2) Renomear as colunas
df = df.rename(columns= {
    "ts": "end_time",
    "master_metadata_album_artist_name": "artist_name",
    "master_metadata_track_name": "track_name",
    "ms_played": "ms_played"
})

# 3) Criar o banco de dados SQLite
conn = sqlite3.connect("spotify_streams.db")

# 4) Gravar a tabela
df.to_sql("spotify_streams", conn, if_exists="replace", index=False)

conn.commit()     

print("Tabela 'streams' criada com sucesso no banco de dados 'spotify_streams.db'.")

# 5) Exemplos de consultas SQL
print("\nTop 10 artistas mais ouvidos (ms_played):")
query_top_artists = """
    SELECT artist_name,
       SUM(ms_played) AS total_ms
    FROM spotify_streams
    GROUP BY artist_name
    ORDER BY total_ms DESC
    LIMIT 10;

"""

top_artists = pd.read_sql_query(query_top_artists, conn)
print(top_artists)

print("\nTop 10 músicas mais ouvidas (ms_played):")
query_top_tracks = """
    SELECT track_name,
       SUM(ms_played) AS total_ms
    FROM spotify_streams
    GROUP BY track_name
    ORDER BY total_ms DESC
    LIMIT 10;
"""

top_tracks = pd.read_sql_query(query_top_tracks, conn)
print(top_tracks)

# Fechar a conexão com o banco de dados
conn.close()
