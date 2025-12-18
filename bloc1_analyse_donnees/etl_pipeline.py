"""
BLOC 1 - Compétence C1.7, C1.8, C1.9
Pipeline ETL (Extract, Transform, Load)

Objectif: Agréger différentes sources de données, les nettoyer et les charger
dans la base de données PostgreSQL de manière optimisée.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
from pathlib import Path
from typing import Dict, List, Tuple
import warnings
from faker import Faker
import random
warnings.filterwarnings('ignore')

fake = Faker('fr_FR')
Faker.seed(42)
random.seed(42)
np.random.seed(42)


class ETLPipeline:
    """
    Pipeline ETL complet pour le système de détection de fraude.
    
    Couvre les compétences:
    - C1.7: Agréger différentes sources de données (CSV, JSON, Excel, API)
    - C1.8: Analyser et corriger anomalies/valeurs manquantes
    - C1.9: Alimenter base de données avec données nettoyées
    """
    
    def __init__(self):
        """Initialisation du pipeline ETL"""
        self.data_sources = {}
        self.cleaned_data = {}
        self.stats_nettoyage = {}
        
    def generer_donnees_test(self, nb_transactions: int = 10000):
        """
        Génère des données de test réalistes pour la démo
        (En production: connexion aux vraies sources)
        
        Args:
            nb_transactions: Nombre de transactions à générer
        """
        print("=" * 80)
        print("🏭 GÉNÉRATION DONNÉES DE TEST")
        print("=" * 80)
        
        # 1. Transactions CSV
        print("\n📄 Génération fichier: transactions.csv")
        
        transactions = []
        for i in range(nb_transactions):
            # 0.25% de fraudes (réaliste)
            is_fraud = random.random() < 0.0025
            
            montant = random.uniform(5, 2000) if not is_fraud else random.uniform(500, 5000)
            
            # Valeurs manquantes intentionnelles (3%)
            ville = fake.city() if random.random() > 0.03 else None
            merchant_cat = random.choice([
                'SUPERMARCHE', 'RESTAURANT', 'ESSENCE', 'VETEMENTS', 
                'PHARMACIE', 'LOISIRS', None
            ])
            
            trans = {
                'transaction_id': f'TRX_{i:08d}',
                'client_id': f'CLIENT_{random.randint(1, 5000):06d}',
                'montant': round(montant, 2),
                'devise': 'EUR',
                'date_heure': fake.date_time_between(
                    start_date='-30d', 
                    end_date='now'
                ).isoformat(),
                'type_transaction': random.choice(['PAIEMENT', 'RETRAIT', 'VIREMENT']),
                'type_carte': random.choice(['DEBIT', 'CREDIT', 'PREPAYEE']),
                'merchant_id': f'MERCHANT_{random.randint(1, 500):05d}',
                'merchant_category': merchant_cat,
                'pays': 'FR',
                'ville': ville,
                'latitude': round(random.uniform(42, 51), 6) if ville else None,
                'longitude': round(random.uniform(-4, 8), 6) if ville else None,
                'canal': random.choice(['WEB', 'MOBILE', 'ATM', 'POS']),
                'is_fraud': is_fraud
            }
            transactions.append(trans)
        
        df_transactions = pd.DataFrame(transactions)
        Path('data/raw').mkdir(parents=True, exist_ok=True)
        df_transactions.to_csv('data/raw/transactions.csv', index=False)
        print(f"   ✅ {len(df_transactions):,} transactions générées")
        print(f"   📊 Taux fraude: {df_transactions['is_fraud'].sum() / len(df_transactions) * 100:.2f}%")
        
        # 2. Données clients Excel
        print("\n📊 Génération fichier: clients.xlsx")
        
        clients_uniques = df_transactions['client_id'].unique()
        clients = []
        for client_id in clients_uniques:
            # Valeurs manquantes intentionnelles (11%)
            score_credit = random.randint(300, 850) if random.random() > 0.11 else None
            
            client = {
                'client_id': client_id,
                'age': random.randint(18, 80),
                'sexe': random.choice(['M', 'F', 'X']),
                'code_postal': fake.postcode()[:5],
                'anciennete_mois': random.randint(1, 120),
                'nb_produits': random.randint(1, 5),
                'revenu_annuel_tranche': random.choice([
                    '0-20K', '20-40K', '40-60K', '60-100K', '>100K'
                ]),
                'score_credit': score_credit,
                'date_derniere_fraude': fake.date_between(
                    start_date='-2y', 
                    end_date='today'
                ) if random.random() < 0.05 else None
            }
            clients.append(client)
        
        df_clients = pd.DataFrame(clients)
        df_clients.to_excel('data/raw/clients.xlsx', index=False)
        print(f"   ✅ {len(df_clients):,} clients générés")
        
        # 3. Logs comportementaux JSON
        print("\n📱 Génération fichier: logs_comportement.json")
        
        logs = []
        for i in range(nb_transactions * 5):  # 5 logs par transaction en moyenne
            log = {
                'session_id': f'SESSION_{i:08d}',
                'client_id': random.choice(clients_uniques),
                'timestamp': fake.date_time_between(
                    start_date='-30d', 
                    end_date='now'
                ).isoformat(),
                'action': random.choice([
                    'login', 'navigation_compte', 'consultation_solde', 
                    'transaction', 'logout'
                ]),
                'device_type': random.choice(['mobile', 'desktop', 'tablet']),
                'os': random.choice(['iOS', 'Android', 'Windows', 'macOS']),
                'browser': random.choice(['Chrome', 'Safari', 'Firefox', 'Edge']),
                'ip_address': fake.ipv4(),
                'duree_session_sec': random.randint(30, 3600)
            }
            logs.append(log)
        
        with open('data/raw/logs_comportement.json', 'w') as f:
            json.dump(logs, f, indent=2)
        print(f"   ✅ {len(logs):,} logs comportementaux générés")
        
        print("\n✅ Génération données de test terminée\n")
    
    def extraire_donnees(self) -> Dict[str, pd.DataFrame]:
        """
        C1.7: Extraction des données depuis différentes sources
        
        Returns:
            Dict contenant les DataFrames de chaque source
        """
        print("=" * 80)
        print("📥 EXTRACTION DES DONNÉES (ETL - Extract)")
        print("=" * 80)
        
        # Source 1: CSV
        print("\n📄 Extraction: transactions.csv")
        df_transactions = pd.read_csv('data/raw/transactions.csv')
        print(f"   ✅ {len(df_transactions):,} transactions extraites")
        print(f"   📊 Colonnes: {list(df_transactions.columns)}")
        self.data_sources['transactions'] = df_transactions
        
        # Source 2: Excel
        print("\n📊 Extraction: clients.xlsx")
        df_clients = pd.read_excel('data/raw/clients.xlsx')
        print(f"   ✅ {len(df_clients):,} clients extraits")
        print(f"   📊 Colonnes: {list(df_clients.columns)}")
        self.data_sources['clients'] = df_clients
        
        # Source 3: JSON
        print("\n📱 Extraction: logs_comportement.json")
        with open('data/raw/logs_comportement.json', 'r') as f:
            logs = json.load(f)
        df_logs = pd.DataFrame(logs)
        print(f"   ✅ {len(df_logs):,} logs extraits")
        print(f"   📊 Colonnes: {list(df_logs.columns)}")
        self.data_sources['logs'] = df_logs
        
        print("\n✅ Extraction terminée: 3 sources chargées\n")
        return self.data_sources
    
    def analyser_qualite_donnees(self, df: pd.DataFrame, nom_source: str) -> Dict:
        """
        C1.8: Analyse de la qualité des données
        
        Args:
            df: DataFrame à analyser
            nom_source: Nom de la source de données
            
        Returns:
            Dict avec statistiques de qualité
        """
        stats = {
            'nom': nom_source,
            'nb_lignes': len(df),
            'nb_colonnes': len(df.columns),
            'valeurs_manquantes': {},
            'doublons': 0,
            'anomalies': {}
        }
        
        # Valeurs manquantes
        for col in df.columns:
            nb_nan = df[col].isna().sum()
            if nb_nan > 0:
                pct = (nb_nan / len(df)) * 100
                stats['valeurs_manquantes'][col] = {
                    'count': int(nb_nan),
                    'pct': round(pct, 2)
                }
        
        # Doublons
        stats['doublons'] = int(df.duplicated().sum())
        
        # Anomalies spécifiques
        if 'montant' in df.columns:
            stats['anomalies']['montants_negatifs'] = int((df['montant'] < 0).sum())
            stats['anomalies']['montants_extremes'] = int((df['montant'] > 10000).sum())
        
        if 'latitude' in df.columns:
            invalides = ((df['latitude'] < -90) | (df['latitude'] > 90)).sum()
            stats['anomalies']['coordonnees_invalides'] = int(invalides)
        
        return stats
    
    def nettoyer_donnees(self) -> Dict[str, pd.DataFrame]:
        """
        C1.8: Nettoyage et correction des données
        
        Returns:
            Dict avec DataFrames nettoyés
        """
        print("=" * 80)
        print("🧹 NETTOYAGE DES DONNÉES (ETL - Transform)")
        print("=" * 80)
        
        # 1. Nettoyage transactions
        print("\n📄 Nettoyage: Transactions")
        df_trans = self.data_sources['transactions'].copy()
        
        # Analyse qualité avant
        stats_avant = self.analyser_qualite_donnees(df_trans, 'transactions')
        print(f"   📊 Avant nettoyage:")
        print(f"      • Lignes: {stats_avant['nb_lignes']:,}")
        print(f"      • Valeurs manquantes: {sum(v['count'] for v in stats_avant['valeurs_manquantes'].values())}")
        print(f"      • Doublons: {stats_avant['doublons']}")
        
        # Correction valeurs manquantes QUANTITATIVES: médiane
        if 'latitude' in df_trans.columns:
            avant_nan = df_trans['latitude'].isna().sum()
            df_trans['latitude'].fillna(df_trans['latitude'].median(), inplace=True)
            df_trans['longitude'].fillna(df_trans['longitude'].median(), inplace=True)
            print(f"      ✅ Latitude/Longitude: {avant_nan} valeurs imputées (médiane)")
        
        # Correction valeurs manquantes QUALITATIVES: mode ou 'INCONNU'
        if 'ville' in df_trans.columns:
            avant_nan = df_trans['ville'].isna().sum()
            df_trans['ville'].fillna('VILLE_INCONNUE', inplace=True)
            print(f"      ✅ Ville: {avant_nan} valeurs imputées ('VILLE_INCONNUE')")
        
        if 'merchant_category' in df_trans.columns:
            avant_nan = df_trans['merchant_category'].isna().sum()
            # Imputation par mode (catégorie la plus fréquente)
            mode_cat = df_trans['merchant_category'].mode()[0] if not df_trans['merchant_category'].mode().empty else 'AUTRE'
            df_trans['merchant_category'].fillna(mode_cat, inplace=True)
            print(f"      ✅ Merchant category: {avant_nan} valeurs imputées (mode='{mode_cat}')")
        
        # Suppression doublons
        avant_dupl = len(df_trans)
        df_trans.drop_duplicates(subset=['transaction_id'], keep='first', inplace=True)
        nb_dupl_suppr = avant_dupl - len(df_trans)
        if nb_dupl_suppr > 0:
            print(f"      ✅ Doublons: {nb_dupl_suppr} lignes supprimées")
        
        # Correction anomalies montants
        avant_negatifs = (df_trans['montant'] < 0).sum()
        df_trans = df_trans[df_trans['montant'] >= 0]
        if avant_negatifs > 0:
            print(f"      ✅ Montants négatifs: {avant_negatifs} transactions supprimées")
        
        # Conversion types
        df_trans['date_heure'] = pd.to_datetime(df_trans['date_heure'])
        print(f"      ✅ Type date_heure converti en datetime")
        
        stats_apres = self.analyser_qualite_donnees(df_trans, 'transactions')
        print(f"   📊 Après nettoyage:")
        print(f"      • Lignes: {stats_apres['nb_lignes']:,}")
        print(f"      • Valeurs manquantes: {sum(v['count'] for v in stats_apres['valeurs_manquantes'].values())}")
        
        self.cleaned_data['transactions'] = df_trans
        self.stats_nettoyage['transactions'] = {
            'avant': stats_avant,
            'apres': stats_apres
        }
        
        # 2. Nettoyage clients
        print("\n📊 Nettoyage: Clients")
        df_clients = self.data_sources['clients'].copy()
        
        stats_avant = self.analyser_qualite_donnees(df_clients, 'clients')
        print(f"   📊 Avant nettoyage:")
        print(f"      • Lignes: {stats_avant['nb_lignes']:,}")
        print(f"      • Valeurs manquantes: {sum(v['count'] for v in stats_avant['valeurs_manquantes'].values())}")
        
        # Score crédit: imputation médiane
        if 'score_credit' in df_clients.columns:
            avant_nan = df_clients['score_credit'].isna().sum()
            mediane = df_clients['score_credit'].median()
            df_clients['score_credit'].fillna(mediane, inplace=True)
            print(f"      ✅ Score crédit: {avant_nan} valeurs imputées (médiane={mediane:.0f})")
        
        # Date dernière fraude: garder NULL si pas de fraude
        # (NULL a du sens ici)
        
        stats_apres = self.analyser_qualite_donnees(df_clients, 'clients')
        print(f"   📊 Après nettoyage:")
        print(f"      • Valeurs manquantes: {sum(v['count'] for v in stats_apres['valeurs_manquantes'].values())}")
        
        self.cleaned_data['clients'] = df_clients
        self.stats_nettoyage['clients'] = {
            'avant': stats_avant,
            'apres': stats_apres
        }
        
        # 3. Nettoyage logs
        print("\n📱 Nettoyage: Logs comportementaux")
        df_logs = self.data_sources['logs'].copy()
        
        # Conversion timestamp
        df_logs['timestamp'] = pd.to_datetime(df_logs['timestamp'])
        
        # Suppression doublons stricts
        avant_dupl = len(df_logs)
        df_logs.drop_duplicates(inplace=True)
        nb_dupl_suppr = avant_dupl - len(df_logs)
        if nb_dupl_suppr > 0:
            print(f"      ✅ Doublons: {nb_dupl_suppr} logs supprimés")
        
        self.cleaned_data['logs'] = df_logs
        
        print("\n✅ Nettoyage terminé: Données prêtes pour chargement\n")
        return self.cleaned_data
    
    def agréger_données(self) -> pd.DataFrame:
        """
        C1.7: Agrégation des différentes sources
        Jointure transactions + clients + features logs
        
        Returns:
            DataFrame agrégé
        """
        print("=" * 80)
        print("🔗 AGRÉGATION DES SOURCES DE DONNÉES")
        print("=" * 80)
        
        df_trans = self.cleaned_data['transactions']
        df_clients = self.cleaned_data['clients']
        df_logs = self.cleaned_data['logs']
        
        # Feature engineering sur logs: nb sessions par client
        print("\n🔧 Création features depuis logs...")
        logs_agg = df_logs.groupby('client_id').agg({
            'session_id': 'count',
            'duree_session_sec': 'mean'
        }).reset_index()
        logs_agg.columns = ['client_id', 'nb_sessions', 'duree_moy_session_sec']
        print(f"   ✅ Features créées: nb_sessions, duree_moy_session_sec")
        
        # Jointure transactions + clients
        print("\n🔗 Jointure: Transactions LEFT JOIN Clients")
        df_merged = df_trans.merge(
            df_clients,
            on='client_id',
            how='left',
            suffixes=('', '_client')
        )
        print(f"   ✅ {len(df_merged):,} lignes après jointure")
        
        # Jointure avec features logs
        print("\n🔗 Jointure: + Features Logs")
        df_merged = df_merged.merge(
            logs_agg,
            on='client_id',
            how='left'
        )
        
        # Imputation valeurs manquantes features logs (clients sans logs)
        df_merged['nb_sessions'].fillna(0, inplace=True)
        df_merged['duree_moy_session_sec'].fillna(0, inplace=True)
        
        print(f"   ✅ {len(df_merged):,} lignes finales")
        print(f"   📊 Nombre total de colonnes: {len(df_merged.columns)}")
        
        print(f"\n   📋 Aperçu colonnes finales:")
        for i, col in enumerate(df_merged.columns, 1):
            print(f"      {i:2d}. {col}")
        
        self.cleaned_data['dataset_final'] = df_merged
        
        print("\n✅ Agrégation terminée\n")
        return df_merged
    
    def charger_donnees(self, simulate: bool = True) -> Dict:
        """
        C1.9: Chargement des données dans PostgreSQL
        
        Args:
            simulate: Si True, simule le chargement (sans vraie BDD)
            
        Returns:
            Dict avec statistiques de chargement
        """
        print("=" * 80)
        print("💾 CHARGEMENT DES DONNÉES (ETL - Load)")
        print("=" * 80)
        
        stats_chargement = {}
        
        for table_name, df in self.cleaned_data.items():
            print(f"\n📤 Chargement table: {table_name}")
            
            if simulate:
                # Simulation: sauvegarde en CSV dans data/processed
                Path('data/processed').mkdir(parents=True, exist_ok=True)
                output_path = f'data/processed/{table_name}_clean.csv'
                df.to_csv(output_path, index=False)
                
                print(f"   ℹ️  Mode simulation: données sauvées en {output_path}")
                print(f"   📊 Lignes: {len(df):,}")
                print(f"   📊 Colonnes: {len(df.columns)}")
                
                # Vérification intégrité (équivalent COUNT(*) en SQL)
                nb_lignes_source = len(df)
                nb_lignes_fichier = len(pd.read_csv(output_path))
                
                if nb_lignes_source == nb_lignes_fichier:
                    print(f"   ✅ Vérification intégrité: {nb_lignes_source:,} = {nb_lignes_fichier:,}")
                else:
                    print(f"   ❌ ERREUR: {nb_lignes_source} ≠ {nb_lignes_fichier}")
                
                stats_chargement[table_name] = {
                    'lignes': nb_lignes_source,
                    'colonnes': len(df.columns),
                    'fichier': output_path,
                    'status': 'OK'
                }
            else:
                # Chargement réel PostgreSQL (nécessite connexion)
                # from sqlalchemy import create_engine
                # engine = create_engine('postgresql://...')
                # df.to_sql(table_name, engine, if_exists='append', index=False)
                print("   ⚠️  Chargement PostgreSQL nécessite connexion configurée")
        
        print(f"\n{'='*80}")
        print("✅ CHARGEMENT TERMINÉ")
        print(f"{'='*80}\n")
        
        return stats_chargement
    
    def generer_rapport_etl(self) -> str:
        """
        Génère un rapport complet du pipeline ETL
        
        Returns:
            Chemin du fichier rapport
        """
        rapport = {
            'date_execution': datetime.now().isoformat(),
            'etapes': ['Extract', 'Transform', 'Load'],
            'sources_extraites': list(self.data_sources.keys()),
            'stats_nettoyage': self.stats_nettoyage,
            'donnees_finales': {
                name: {
                    'nb_lignes': len(df),
                    'nb_colonnes': len(df.columns)
                }
                for name, df in self.cleaned_data.items()
            }
        }
        
        output_path = 'data/processed/rapport_etl.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(rapport, f, indent=2, ensure_ascii=False)
        
        print(f"📄 Rapport ETL généré: {output_path}")
        return output_path


def main():
    """Point d'entrée principal"""
    print("\n" + "🚀"*40)
    print("BLOC 1 - PIPELINE ETL COMPLET")
    print("Compétences C1.7, C1.8, C1.9")
    print("🚀"*40 + "\n")
    
    # Initialisation
    etl = ETLPipeline()
    
    # Génération données test
    print("\n📋 Étape 0: Génération données de test")
    etl.generer_donnees_test(nb_transactions=10000)
    
    # C1.7: Extraction
    print("\n📋 Étape 1: Extraction (Extract)")
    etl.extraire_donnees()
    
    # C1.8: Nettoyage (Transform)
    print("\n📋 Étape 2: Nettoyage (Transform)")
    etl.nettoyer_donnees()
    
    # C1.7: Agrégation
    print("\n📋 Étape 3: Agrégation sources")
    etl.agréger_données()
    
    # C1.9: Chargement (Load)
    print("\n📋 Étape 4: Chargement (Load)")
    stats = etl.charger_donnees(simulate=True)
    
    # Rapport
    print("\n📋 Étape 5: Génération rapport ETL")
    etl.generer_rapport_etl()
    
    print(f"\n{'='*80}")
    print("✅ PIPELINE ETL EXÉCUTÉ AVEC SUCCÈS")
    print(f"{'='*80}\n")
    
    # Résumé
    print("📊 RÉSUMÉ:")
    for table, stat in stats.items():
        print(f"   • {table}: {stat['lignes']:,} lignes chargées")
    print()


if __name__ == "__main__":
    main()