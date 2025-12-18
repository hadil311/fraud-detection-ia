"""
BLOC 1 - Compétence C1.1 & C1.2
Audit et Cartographie des Données Bancaires

Objectif: Analyser le besoin client et cartographier toutes les sources de données
pour évaluer la faisabilité technique du projet de détection de fraude.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import json
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

class AuditDonneesBancaires:
    """
    Classe pour auditer et cartographier les sources de données bancaires.
    
    Couvre les compétences:
    - C1.1: Analyser le besoin client
    - C1.2: Réaliser audit et cartographie des données
    """
    
    def __init__(self, config_path: str = None):
        """Initialisation de l'audit"""
        self.sources_donnees = {}
        self.rapport_audit = {}
        self.statistiques_volumetrie = {}
        
    def analyser_besoin_client(self) -> Dict:
        """
        C1.1: Analyser le besoin du client bancaire
        
        Returns:
            Dict contenant l'analyse complète du besoin
        """
        analyse_besoin = {
            "entreprise": {
                "nom": "Banque Régionale ACME",
                "secteur": "Services Financiers - Banque Retail",
                "problematique": "Perte de 50M€/an en fraudes non détectées",
                "nb_clients": 2_500_000,
                "nb_transactions_jour": 10_000_000
            },
            "besoins_identifies": [
                "Détection automatique fraudes en temps réel",
                "Réduction taux faux positifs (actuellement 15%)",
                "Explicabilité des décisions pour conformité réglementaire",
                "Dashboard temps réel pour analystes fraude",
                "Conformité RGPD et directives bancaires (DSP2)"
            ],
            "contraintes": {
                "latence_max": "< 100ms par transaction",
                "disponibilite": "99.9% (24/7)",
                "faux_positifs_max": "< 2%",
                "recall_min": "> 80%",
                "rgpd": True,
                "pci_dss": True
            },
            "objectifs_business": {
                "reduction_pertes": "90% (45M€/an économisés)",
                "reduction_temps_traitement": "85%",
                "amelioration_satisfaction_client": "+40%",
                "roi_attendu": "> 1000% année 1"
            },
            "ecosysteme_technique": {
                "si_actuel": "IBM Mainframe + Oracle DB",
                "api_disponibles": ["Core Banking", "Payment Gateway", "KYC"],
                "cloud": "Migration AWS en cours",
                "equipes": {
                    "it": 50,
                    "fraude": 25,
                    "data": 5
                }
            }
        }
        
        self.rapport_audit['analyse_besoin'] = analyse_besoin
        
        print("=" * 80)
        print("📊 ANALYSE DU BESOIN CLIENT - DÉTECTION FRAUDE BANCAIRE")
        print("=" * 80)
        print(f"\n🏦 Entreprise: {analyse_besoin['entreprise']['nom']}")
        print(f"💰 Pertes actuelles: 50M€/an")
        print(f"🎯 Objectif: Réduction 90% des fraudes non détectées")
        print(f"\n⚡ Contraintes techniques:")
        for key, value in analyse_besoin['contraintes'].items():
            print(f"   • {key}: {value}")
        
        return analyse_besoin
    
    def cartographier_sources_donnees(self) -> Dict:
        """
        C1.2: Réaliser cartographie complète des sources de données
        
        Returns:
            Dict avec la cartographie de toutes les sources
        """
        cartographie = {
            "source_1_transactions": {
                "nom": "Transactions Bancaires",
                "type": "SQL - PostgreSQL",
                "localisation": "Core Banking System",
                "format": "Base relationnelle",
                "frequence_maj": "Temps réel (streaming)",
                "volumetrie": {
                    "nb_lignes_jour": 10_000_000,
                    "nb_lignes_total": 3_650_000_000,  # 1 an historique
                    "taille_go": 500,
                    "croissance_mensuelle_go": 40
                },
                "variables": {
                    "transaction_id": "VARCHAR(50) - Identifiant unique",
                    "client_id": "VARCHAR(50) - ID client pseudonymisé",
                    "montant": "DECIMAL(10,2) - Montant transaction",
                    "devise": "VARCHAR(3) - Code ISO devise",
                    "date_heure": "TIMESTAMP - Date/heure transaction",
                    "type_transaction": "VARCHAR(50) - Paiement/Retrait/Virement",
                    "type_carte": "VARCHAR(20) - Débit/Crédit/Prépayée",
                    "merchant_id": "VARCHAR(50) - Identifiant commerçant",
                    "merchant_category": "VARCHAR(100) - Catégorie commerce",
                    "pays": "VARCHAR(2) - Code pays ISO",
                    "ville": "VARCHAR(100) - Ville transaction",
                    "latitude": "DECIMAL(9,6) - Coordonnées GPS",
                    "longitude": "DECIMAL(9,6) - Coordonnées GPS",
                    "canal": "VARCHAR(50) - Web/Mobile/ATM/POS",
                    "is_fraud": "BOOLEAN - Label fraude (historique)"
                },
                "nb_variables": 15,
                "qualite": {
                    "completude": 0.97,
                    "valeurs_manquantes_pct": 3,
                    "doublons_pct": 0.01
                }
            },
            "source_2_clients": {
                "nom": "Données Clients (KYC)",
                "type": "SQL - PostgreSQL",
                "localisation": "CRM System",
                "format": "Base relationnelle",
                "frequence_maj": "Hebdomadaire",
                "volumetrie": {
                    "nb_lignes_total": 2_500_000,
                    "taille_go": 5,
                    "croissance_mensuelle_go": 0.2
                },
                "variables": {
                    "client_id": "VARCHAR(50)",
                    "age": "INT",
                    "sexe": "VARCHAR(1)",
                    "code_postal": "VARCHAR(10)",
                    "anciennete_mois": "INT",
                    "nb_produits": "INT",
                    "revenu_annuel_tranche": "VARCHAR(20)",
                    "score_credit": "INT",
                    "date_derniere_fraude": "DATE"
                },
                "nb_variables": 9,
                "qualite": {
                    "completude": 0.89,
                    "valeurs_manquantes_pct": 11,
                    "sensibilite_rgpd": "HAUTE"
                }
            },
            "source_3_comportements": {
                "nom": "Logs Comportementaux Web/Mobile",
                "type": "NoSQL - MongoDB",
                "localisation": "Digital Banking Platform",
                "format": "Documents JSON",
                "frequence_maj": "Temps réel (streaming)",
                "volumetrie": {
                    "nb_documents_jour": 50_000_000,
                    "nb_documents_total": 18_250_000_000,  # 1 an
                    "taille_go": 2000,
                    "croissance_mensuelle_go": 150
                },
                "variables": {
                    "session_id": "string",
                    "client_id": "string",
                    "timestamp": "datetime",
                    "action": "string - login/navigation/transaction",
                    "device_type": "string",
                    "os": "string",
                    "browser": "string",
                    "ip_address": "string - pseudonymisée",
                    "geolocation": "object",
                    "duree_session_sec": "int"
                },
                "nb_variables": 10,
                "qualite": {
                    "completude": 0.95,
                    "valeurs_manquantes_pct": 5
                }
            },
            "source_4_historique_fraudes": {
                "nom": "Historique Fraudes Confirmées",
                "type": "Fichier CSV",
                "localisation": "Équipe Fraude - Sharepoint",
                "format": "CSV (UTF-8)",
                "frequence_maj": "Quotidienne",
                "volumetrie": {
                    "nb_lignes_total": 250_000,  # 0.25% taux fraude
                    "taille_mo": 50,
                    "periode": "3 ans historique"
                },
                "variables": {
                    "fraude_id": "Identifiant unique",
                    "transaction_id": "Lien vers transaction",
                    "type_fraude": "Vol carte/Phishing/Usurpation identité",
                    "montant_perdu": "Montant",
                    "date_detection": "Date",
                    "statut": "Résolu/En cours/Non résolu",
                    "feedback_client": "Texte libre"
                },
                "nb_variables": 7,
                "qualite": {
                    "completude": 0.92,
                    "valeurs_manquantes_pct": 8
                }
            }
        }
        
        self.sources_donnees = cartographie
        self.rapport_audit['cartographie'] = cartographie
        
        # Calcul statistiques globales
        volumetrie_totale = self._calculer_volumetrie_totale(cartographie)
        self.statistiques_volumetrie = volumetrie_totale
        
        print("\n" + "=" * 80)
        print("📦 CARTOGRAPHIE DES SOURCES DE DONNÉES")
        print("=" * 80)
        
        for source_id, source in cartographie.items():
            print(f"\n📁 {source['nom']}")
            print(f"   Type: {source['type']}")
            print(f"   Fréquence MAJ: {source['frequence_maj']}")
            if 'nb_lignes_total' in source['volumetrie']:
                print(f"   Volumétrie: {source['volumetrie']['nb_lignes_total']:,} lignes")
            elif 'nb_documents_total' in source['volumetrie']:
                print(f"   Volumétrie: {source['volumetrie']['nb_documents_total']:,} documents")
            print(f"   Taille: {source['volumetrie'].get('taille_go', source['volumetrie'].get('taille_mo', 0))} GB/MB")
            print(f"   Variables: {source['nb_variables']}")
            print(f"   Qualité (complétude): {source['qualite']['completude']*100:.1f}%")
        
        print(f"\n{'='*80}")
        print("📊 VOLUMÉTRIE TOTALE")
        print(f"{'='*80}")
        print(f"Total observations: {volumetrie_totale['total_observations']:,}")
        print(f"Total variables: {volumetrie_totale['total_variables']}")
        print(f"Taille totale: {volumetrie_totale['taille_totale_go']:.1f} GB")
        print(f"Croissance mensuelle: {volumetrie_totale['croissance_mensuelle_go']:.1f} GB")
        
        return cartographie
    
    def _calculer_volumetrie_totale(self, cartographie: Dict) -> Dict:
        """Calcule les statistiques de volumétrie totale"""
        total_obs = 0
        total_vars = 0
        total_taille = 0
        total_croissance = 0
        
        for source in cartographie.values():
            # Observations
            if 'nb_lignes_total' in source['volumetrie']:
                total_obs += source['volumetrie']['nb_lignes_total']
            elif 'nb_documents_total' in source['volumetrie']:
                total_obs += source['volumetrie']['nb_documents_total']
            
            # Variables
            total_vars += source['nb_variables']
            
            # Taille
            if 'taille_go' in source['volumetrie']:
                total_taille += source['volumetrie']['taille_go']
            elif 'taille_mo' in source['volumetrie']:
                total_taille += source['volumetrie']['taille_mo'] / 1024
            
            # Croissance
            if 'croissance_mensuelle_go' in source['volumetrie']:
                total_croissance += source['volumetrie']['croissance_mensuelle_go']
        
        return {
            'total_observations': total_obs,
            'total_variables': total_vars,
            'taille_totale_go': total_taille,
            'croissance_mensuelle_go': total_croissance
        }
    
    def evaluer_faisabilite_technique(self) -> Dict:
        """
        Évalue la faisabilité technique du projet basée sur l'audit
        
        Returns:
            Dict avec évaluation de faisabilité
        """
        volumetrie = self.statistiques_volumetrie
        
        faisabilite = {
            "volumetrie": {
                "statut": "✅ FAISABLE" if volumetrie['total_observations'] < 50_000_000_000 else "⚠️ ATTENTION",
                "commentaire": "Volumétrie importante mais gérable avec architecture Big Data",
                "recommandation": "PostgreSQL avec partitionnement + MongoDB pour logs"
            },
            "qualite_donnees": {
                "statut": "✅ BONNE",
                "completude_moyenne": 0.93,
                "commentaire": "Qualité suffisante pour ML (>90%)",
                "actions_requises": [
                    "Imputation valeurs manquantes",
                    "Nettoyage doublons",
                    "Validation cohérence géographique"
                ]
            },
            "tempo_realite": {
                "statut": "✅ FAISABLE",
                "latence_requise_ms": 100,
                "latence_attendue_ms": 45,
                "commentaire": "Architecture streaming Kafka + Redis permettra respect latence"
            },
            "rgpd_ethique": {
                "statut": "⚠️ ATTENTION HAUTE",
                "sensibilite": "TRÈS HAUTE - Données bancaires",
                "actions_requises": [
                    "Pseudonymisation obligatoire",
                    "Chiffrement AES-256 au repos",
                    "Audit DPO avant déploiement",
                    "Droit à l'explication (SHAP)",
                    "Registre traitements RGPD"
                ]
            },
            "ml_feasibility": {
                "statut": "✅ EXCELLENT",
                "historique_labels": 250_000,
                "taux_fraude": 0.0025,  # 0.25%
                "commentaire": "Suffisant pour ML supervisé avec techniques SMOTE",
                "algorithms_recommandes": [
                    "Random Forest (baseline)",
                    "XGBoost (recommandé)",
                    "LSTM (séquences temporelles)",
                    "Isolation Forest (anomalies nouvelles)"
                ]
            },
            "synthese": {
                "faisabilite_globale": "✅ PROJET FAISABLE",
                "risques_majeurs": [
                    "Conformité RGPD (mitigé par pseudonymisation)",
                    "Déséquilibre classes 0.25% (mitigé par SMOTE)",
                    "Latence temps réel (mitigé par Redis)"
                ],
                "budget_estime": "80-100k€",
                "duree_estimee_mois": 6,
                "equipe_recommandee": {
                    "data_scientists": 3,
                    "data_engineers": 2,
                    "devops": 1,
                    "juriste_rgpd": 1,
                    "responsable_ethique": 1,
                    "analystes_metier": 2
                }
            }
        }
        
        self.rapport_audit['faisabilite'] = faisabilite
        
        print(f"\n{'='*80}")
        print("🎯 ÉVALUATION DE FAISABILITÉ TECHNIQUE")
        print(f"{'='*80}")
        print(f"\n📊 Volumétrie: {faisabilite['volumetrie']['statut']}")
        print(f"   {faisabilite['volumetrie']['commentaire']}")
        print(f"\n🎨 Qualité données: {faisabilite['qualite_donnees']['statut']}")
        print(f"   Complétude: {faisabilite['qualite_donnees']['completude_moyenne']*100:.1f}%")
        print(f"\n⚡ Temps réel: {faisabilite['tempo_realite']['statut']}")
        print(f"   Latence attendue: {faisabilite['tempo_realite']['latence_attendue_ms']}ms")
        print(f"\n🔒 RGPD/Éthique: {faisabilite['rgpd_ethique']['statut']}")
        print(f"   Sensibilité: {faisabilite['rgpd_ethique']['sensibilite']}")
        print(f"\n🤖 ML Feasibility: {faisabilite['ml_feasibility']['statut']}")
        print(f"   Historique labels: {faisabilite['ml_feasibility']['historique_labels']:,}")
        
        print(f"\n{'='*80}")
        print(f"✅ SYNTHÈSE: {faisabilite['synthese']['faisabilite_globale']}")
        print(f"💰 Budget estimé: {faisabilite['synthese']['budget_estime']}")
        print(f"⏱️  Durée estimée: {faisabilite['synthese']['duree_estimee_mois']} mois")
        print(f"{'='*80}\n")
        
        return faisabilite
    
    def generer_rapport_audit(self, output_path: str = None) -> str:
        """
        Génère le rapport d'audit complet au format JSON
        
        Args:
            output_path: Chemin fichier sortie
            
        Returns:
            Chemin du fichier généré
        """
        if output_path is None:
            output_path = "data/raw/rapport_audit_donnees.json"
        
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        rapport_complet = {
            "date_audit": datetime.now().isoformat(),
            "version": "1.0",
            "auditeur": "Chef de Projet Data & IA",
            **self.rapport_audit
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(rapport_complet, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Rapport d'audit généré: {output_path}")
        return output_path


def main():
    """Point d'entrée principal"""
    print("\n" + "🚀"*40)
    print("BLOC 1 - AUDIT ET CARTOGRAPHIE DES DONNÉES")
    print("Compétences C1.1 & C1.2")
    print("🚀"*40 + "\n")
    
    # Initialisation
    audit = AuditDonneesBancaires()
    
    # C1.1: Analyse besoin client
    print("\n📋 Étape 1: Analyse du besoin client")
    audit.analyser_besoin_client()
    
    # C1.2: Cartographie sources données
    print("\n📋 Étape 2: Cartographie des sources de données")
    audit.cartographier_sources_donnees()
    
    # Évaluation faisabilité
    print("\n📋 Étape 3: Évaluation de la faisabilité technique")
    audit.evaluer_faisabilite_technique()
    
    # Génération rapport
    print("\n📋 Étape 4: Génération du rapport d'audit")
    rapport_path = audit.generer_rapport_audit()
    
    print(f"\n{'='*80}")
    print("✅ AUDIT TERMINÉ AVEC SUCCÈS")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()