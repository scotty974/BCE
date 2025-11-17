from neo4j import GraphDatabase
import json
import logging
import os
from typing import Dict, List, Any

class BCENeo4jLoader:
    """
    Charge les données BCE extraites dans Neo4j.
    Lit les fichiers JSON temporaires et crée le graphe de connaissances.
    """
    
    def __init__(self, neo4j_uri, neo4j_user, neo4j_password):
        """
        Initialise la connexion Neo4j.
        
        Args:
            neo4j_uri (str): URI de Neo4j (ex: 'bolt://localhost:7687')
            neo4j_user (str): Utilisateur Neo4j
            neo4j_password (str): Mot de passe Neo4j
        """
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.logger = logging.getLogger(__name__)
        
    def close(self):
        """Ferme la connexion Neo4j"""
        self.driver.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def load_json_file(self, json_path):
        """
        Charge un fichier JSON.
        
        Args:
            json_path (str): Chemin du fichier JSON
            
        Returns:
            dict: Données chargées
        """
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def create_constraints(self):
        """Crée les contraintes d'unicité dans Neo4j"""
        with self.driver.session() as session:
            constraints = [
                "CREATE CONSTRAINT entreprise_numero IF NOT EXISTS FOR (e:Entreprise) REQUIRE e.numero IS UNIQUE",
                "CREATE CONSTRAINT personne_nom IF NOT EXISTS FOR (p:Personne) REQUIRE (p.nom, p.prenom) IS UNIQUE",
                "CREATE CONSTRAINT adresse_id IF NOT EXISTS FOR (a:Adresse) REQUIRE a.id IS UNIQUE",
                "CREATE CONSTRAINT activite_code IF NOT EXISTS FOR (a:Activite) REQUIRE (a.code, a.version) IS UNIQUE"
            ]
            
            for constraint in constraints:
                try:
                    session.run(constraint)
                    self.logger.info(f"Contrainte créée: {constraint}")
                except Exception as e:
                    self.logger.warning(f"Contrainte déjà existante ou erreur: {e}")
    
    def create_indexes(self):
        """Crée les index pour améliorer les performances"""
        with self.driver.session() as session:
            indexes = [
                "CREATE INDEX entreprise_statut IF NOT EXISTS FOR (e:Entreprise) ON (e.statut)",
                "CREATE INDEX personne_fonction IF NOT EXISTS FOR (p:Personne) ON (p.fonction)",
                "CREATE INDEX activite_description IF NOT EXISTS FOR (a:Activite) ON (a.description)"
            ]
            
            for index in indexes:
                try:
                    session.run(index)
                    self.logger.info(f"Index créé: {index}")
                except Exception as e:
                    self.logger.warning(f"Index déjà existant ou erreur: {e}")
    
    def parse_nom_personne(self, nom_complet):
        """
        Parse un nom complet en nom et prénom.
        Format attendu: "Nom , Prénom"
        """
        parts = nom_complet.split(',')
        nom = parts[0].strip() if len(parts) > 0 else nom_complet
        prenom = parts[1].strip() if len(parts) > 1 else ''
        return nom, prenom
    
    def create_entreprise_node(self, session, data):
        """Crée le nœud Entreprise principal"""
        identification = data.get('identification', {})
        denomination = data.get('denomination', {})
        donnees_juridiques = data.get('donnees_juridiques', {})
        etablissements = data.get('etablissements', {})
        donnees_financieres = data.get('donnees_financieres', {})
        
        # Récupérer les dénominations
        noms = denomination.get('noms', [])
        nom_principal = noms[0].get('nom') if noms else 'Inconnu'
        
        abreviations = denomination.get('abreviations', [])
        abreviation_principale = abreviations[0].get('abréviation') if abreviations else None
        
        query = """
        MERGE (e:Entreprise {numero: $numero})
        SET e.nom = $nom,
            e.abreviation = $abreviation,
            e.statut = $statut,
            e.type = $type,
            e.forme_legale = $forme_legale,
            e.forme_legale_depuis = $forme_legale_depuis,
            e.situation_juridique = $situation,
            e.situation_depuis = $situation_depuis,
            e.date_debut = $date_debut,
            e.nb_etablissements = $nb_etablissements,
            e.capital = $capital,
            e.assemblee_generale = $assemblee_generale,
            e.fin_annee_comptable = $fin_annee_comptable,
            e.updated_at = datetime()
        RETURN e
        """
        
        result = session.run(query, 
            numero=identification.get('numero', 'INCONNU'),
            nom=nom_principal,
            abreviation=abreviation_principale,
            statut=identification.get('statut'),
            type=identification.get('type'),
            forme_legale=donnees_juridiques.get('forme_legale'),
            forme_legale_depuis=donnees_juridiques.get('forme_legale_depuis'),
            situation=donnees_juridiques.get('situation'),
            situation_depuis=donnees_juridiques.get('situation_depuis'),
            date_debut=donnees_juridiques.get('date_debut'),
            nb_etablissements=etablissements.get('nombre'),
            capital=donnees_financieres.get('capital'),
            assemblee_generale=donnees_financieres.get('assemblee_generale'),
            fin_annee_comptable=donnees_financieres.get('fin_annee_comptable')
        )
        
        return result.single()['e']
    
    def create_denominations(self, session, entreprise_numero, data):
        """Crée les nœuds Dénomination et les relations"""
        denomination = data.get('denomination', {})
        noms = denomination.get('noms', [])
        
        for nom_data in noms:
            query = """
            MATCH (e:Entreprise {numero: $numero})
            MERGE (d:Denomination {nom: $nom, langue: $langue})
            SET d.depuis = $depuis
            MERGE (e)-[r:A_POUR_DENOMINATION]->(d)
            SET r.depuis = $depuis
            """
            session.run(query,
                numero=entreprise_numero,
                nom=nom_data.get('nom'),
                langue=nom_data.get('langue'),
                depuis=nom_data.get('depuis')
            )
    
    def create_adresse(self, session, entreprise_numero, data):
        """Crée le nœud Adresse et la relation"""
        adresse_siege = data.get('adresse_siege', {})
        
        if not adresse_siege:
            return
        
        # Créer un ID unique pour l'adresse
        adresse_id = f"{adresse_siege.get('rue', '')}_{adresse_siege.get('code_postal_commune', '')}".replace(' ', '_')
        
        query = """
        MATCH (e:Entreprise {numero: $numero})
        MERGE (a:Adresse {id: $adresse_id})
        SET a.rue = $rue,
            a.code_postal_commune = $cp_commune,
            a.depuis = $depuis
        MERGE (e)-[r:SIEGE_A]->(a)
        SET r.depuis = $depuis
        """
        
        session.run(query,
            numero=entreprise_numero,
            adresse_id=adresse_id,
            rue=adresse_siege.get('rue'),
            cp_commune=adresse_siege.get('code_postal_commune'),
            depuis=adresse_siege.get('depuis')
        )
    
    def create_contacts(self, session, entreprise_numero, data):
        """Ajoute les informations de contact à l'entreprise"""
        contacts = data.get('contacts', {})
        
        if not contacts:
            return
        
        query = """
        MATCH (e:Entreprise {numero: $numero})
        SET e.telephone = $telephone,
            e.fax = $fax,
            e.email = $email,
            e.site_web = $site_web
        """
        
        session.run(query,
            numero=entreprise_numero,
            telephone=contacts.get('téléphone'),
            fax=contacts.get('fax'),
            email=contacts.get('e-mail'),
            site_web=contacts.get('adresse_web')
        )
    
    def create_organes(self, session, entreprise_numero, data):
        """Crée les nœuds Personne et les relations de fonction"""
        organes = data.get('organes', [])
        
        for organe in organes:
            nom, prenom = self.parse_nom_personne(organe.get('nom', ''))
            
            query = """
            MATCH (e:Entreprise {numero: $numero})
            MERGE (p:Personne {nom: $nom, prenom: $prenom})
            MERGE (e)-[r:A_POUR_ORGANE]->(p)
            SET r.fonction = $fonction,
                r.depuis = $depuis
            """
            
            session.run(query,
                numero=entreprise_numero,
                nom=nom,
                prenom=prenom,
                fonction=organe.get('fonction'),
                depuis=organe.get('depuis')
            )
    
    def create_qualites(self, session, entreprise_numero, data):
        """Crée les nœuds Qualité et les relations"""
        qualites = data.get('qualites', [])
        
        for qualite in qualites:
            query = """
            MATCH (e:Entreprise {numero: $numero})
            MERGE (q:Qualite {type: $type})
            MERGE (e)-[r:POSSEDE_QUALITE]->(q)
            SET r.valeur = $valeur,
                r.depuis = $depuis
            """
            
            session.run(query,
                numero=entreprise_numero,
                type=qualite.get('type'),
                valeur=qualite.get('valeur'),
                depuis=qualite.get('depuis')
            )
    
    def create_activites(self, session, entreprise_numero, data):
        """Crée les nœuds Activité et les relations"""
        activites = data.get('activites', {})
        
        # Traiter toutes les versions d'activités
        for version_key, activites_list in activites.items():
            version = version_key.split('_')[1] if '_' in version_key else version_key
            type_activite = version_key.split('_')[0].upper() if '_' in version_key else 'TVA'
            
            for activite in activites_list:
                query = """
                MATCH (e:Entreprise {numero: $numero})
                MERGE (a:Activite {code: $code, version: $version})
                SET a.description = $description,
                    a.type = $type
                MERGE (e)-[r:EXERCE_ACTIVITE]->(a)
                SET r.depuis = $depuis
                """
                
                session.run(query,
                    numero=entreprise_numero,
                    code=activite.get('code'),
                    version=version,
                    description=activite.get('description'),
                    type=type_activite,
                    depuis=activite.get('depuis')
                )
    
    def create_liens_entites(self, session, entreprise_numero, data):
        """Crée les relations entre entreprises (absorptions, etc.)"""
        liens = data.get('liens_entites', [])
        
        for lien in liens:
            # Extraire le numéro d'entreprise du lien
            import re
            match = re.search(r'(\d{4}\.\d{3}\.\d{3})', lien)
            if not match:
                continue
            
            numero_lie = match.group(1)
            
            # Déterminer le type de relation
            if 'absorbée par cette entité' in lien:
                # L'entreprise actuelle absorbe l'autre
                query = """
                MATCH (e1:Entreprise {numero: $numero_actuel})
                MERGE (e2:Entreprise {numero: $numero_lie})
                MERGE (e1)-[r:ABSORBE]->(e2)
                SET r.description = $description
                """
            elif 'est absorbée par' in lien:
                # L'entreprise actuelle est absorbée par l'autre
                query = """
                MATCH (e1:Entreprise {numero: $numero_actuel})
                MERGE (e2:Entreprise {numero: $numero_lie})
                MERGE (e2)-[r:ABSORBE]->(e1)
                SET r.description = $description
                """
            else:
                continue
            
            session.run(query,
                numero_actuel=entreprise_numero,
                numero_lie=numero_lie,
                description=lien
            )
    
    def load_bce_data(self, json_path):
        """
        Charge les données BCE depuis un fichier JSON dans Neo4j.
        
        Args:
            json_path (str): Chemin du fichier JSON
        """
        try:
            # Charger les données
            data = self.load_json_file(json_path)
            self.logger.info(f"Chargement des données depuis {json_path}")
            
            # Récupérer le numéro d'entreprise
            entreprise_numero = data.get('identification', {}).get('numero')
            if not entreprise_numero:
                self.logger.error(f"Numéro d'entreprise non trouvé dans {json_path}")
                return
            
            # Créer les données dans Neo4j
            with self.driver.session() as session:
                # 1. Créer l'entreprise
                self.create_entreprise_node(session, data)
                self.logger.info(f"Entreprise {entreprise_numero} créée")
                
                # 2. Créer les dénominations
                self.create_denominations(session, entreprise_numero, data)
                
                # 3. Créer l'adresse
                self.create_adresse(session, entreprise_numero, data)
                
                # 4. Ajouter les contacts
                self.create_contacts(session, entreprise_numero, data)
                
                # 5. Créer les organes
                self.create_organes(session, entreprise_numero, data)
                
                # 6. Créer les qualités
                self.create_qualites(session, entreprise_numero, data)
                
                # 7. Créer les activités
                self.create_activites(session, entreprise_numero, data)
                
                # 8. Créer les liens entre entités
                self.create_liens_entites(session, entreprise_numero, data)
            
            self.logger.info(f"Données de l'entreprise {entreprise_numero} chargées avec succès dans Neo4j")
            
        except Exception as e:
            self.logger.error(f"Erreur lors du chargement de {json_path}: {e}")
            raise
    
    def load_multiple_files(self, json_dir):
        """
        Charge plusieurs fichiers JSON depuis un répertoire.
        
        Args:
            json_dir (str): Chemin du répertoire contenant les fichiers JSON
        """
        # Créer les contraintes et index une seule fois
        self.create_constraints()
        self.create_indexes()
        
        # Parcourir tous les fichiers JSON
        for filename in os.listdir(json_dir):
            if filename.endswith('.json'):
                json_path = os.path.join(json_dir, filename)
                try:
                    self.load_bce_data(json_path)
                except Exception as e:
                    self.logger.error(f"Échec du chargement de {filename}: {e}")
                    continue