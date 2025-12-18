"""
BLOC 1 - Chargement des données CSV dans PostgreSQL
"""

import pandas as pd
from sqlalchemy import create_engine, text
import sys

# ⚠️ REMPLACE PAR TON VRAI MOT DE PASSE POSTGRESQL
PASSWORD = 'hadil3111999'

print("=" * 80)
print("💾 CHARGEMENT DONNÉES DANS POSTGRESQL")
print("=" * 80)

try:
    # Connexion
    print("\n🔗 Connexion à PostgreSQL...")
    engine = create_engine(
        f'postgresql://postgres:{PASSWORD}@localhost:5432/fraud_detection_db'
    )
    
    # Test connexion
    with engine.connect() as conn:
        result = conn.execute(text("SELECT current_database();"))
        db = result.fetchone()[0]
        print(f"   ✅ Connecté à: {db}")
    
    # 1. Charger clients
    print("\n📤 Chargement clients...")
    df_clients = pd.read_csv('data/processed/clients_clean.csv')
    print(f"   📊 Fichier CSV: {len(df_clients):,} clients")
    
    df_clients.to_sql('clients', engine, if_exists='append', index=False)
    print(f"   ✅ {len(df_clients):,} clients chargés dans PostgreSQL")
    
    # 2. Charger transactions
    print("\n📤 Chargement transactions...")
    df_trans = pd.read_csv('data/processed/transactions_clean.csv')
    print(f"   📊 Fichier CSV: {len(df_trans):,} transactions")
    
    # Conversion date
    df_trans['date_heure'] = pd.to_datetime(df_trans['date_heure'])
    
    df_trans.to_sql('transactions_2024_01', engine, if_exists='append', index=False)
    print(f"   ✅ {len(df_trans):,} transactions chargées dans PostgreSQL")
    
    # 3. Vérification finale
    print("\n📊 Vérification dans PostgreSQL...")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM clients;"))
        nb_clients = result.fetchone()[0]
        
        result = conn.execute(text("SELECT COUNT(*) FROM transactions_2024_01;"))
        nb_trans = result.fetchone()[0]
    
    print(f"   Clients en BDD: {nb_clients:,}")
    print(f"   Transactions en BDD: {nb_trans:,}")
    
    print("\n" + "=" * 80)
    print("✅ CHARGEMENT TERMINÉ AVEC SUCCÈS !")
    print("=" * 80)
    print("\n👉 PROCHAINE ÉTAPE:")
    print("   1. Ouvre DBeaver")
    print("   2. Rafraîchis les tables (F5)")
    print("   3. Clique droit sur 'clients' → View Data")
    print("   4. TU VERRAS TES 4,333 CLIENTS ! 🎉")
    print("=" * 80 + "\n")
    
except FileNotFoundError as e:
    print(f"\n❌ ERREUR: Fichier non trouvé")
    print(f"   {e}")
    print("\n💡 Vérifie que les fichiers existent:")
    print("   - data/processed/clients_clean.csv")
    print("   - data/processed/transactions_clean.csv")
    sys.exit(1)
    
except Exception as e:
    print(f"\n❌ ERREUR: {e}")
    print("\n💡 Vérifie:")
    print("   - Que le mot de passe PostgreSQL est correct")
    print("   - Que PostgreSQL tourne")
    print("   - Que la base fraud_detection_db existe")
    sys.exit(1)