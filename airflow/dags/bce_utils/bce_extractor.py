from bs4 import BeautifulSoup
import json
import re
from hdfs import InsecureClient
import logging
from datetime import datetime

class BCEDataExtractor:
    """
    Extracteur de données BCE depuis des fichiers HTML stockés dans HDFS.
    Sauvegarde les données extraites en JSON temporaire pour traitement ultérieur.
    """
    
    def __init__(self, hdfs_url, hdfs_user='hdfs', temp_output_dir='/tmp/bce_extracted'):
        """
        Initialise l'extracteur avec la connexion HDFS.
        
        Args:
            hdfs_url (str): URL du serveur HDFS (ex: 'http://namenode:9870')
            hdfs_user (str): Utilisateur HDFS (défaut: 'hdfs')
            temp_output_dir (str): Répertoire temporaire pour les fichiers JSON extraits
        """
        self.hdfs_client = InsecureClient(hdfs_url, user=hdfs_user)
        self.temp_output_dir = temp_output_dir
        self.logger = logging.getLogger(__name__)
        
    def clean_text(self, text):
        """Nettoie le texte en enlevant les espaces multiples et sauts de ligne"""
        return re.sub(r'\s+', ' ', text).strip()
    
    def extract_date_info(self, text):
        """Extrait la date d'un texte avec 'Depuis le' ou 'depuis le'"""
        match = re.search(r'[Dd]epuis le (.+?)$', text)
        return match.group(1) if match else None
    
    def parse_denomination_cell(self, cell):
        """Parse les dénominations ou abréviations d'une cellule HTML"""
        denominations = []
        
        # Récupérer tout le contenu HTML
        html_content = str(cell)
        
        # Séparer par <br/> ou <br>
        parts = re.split(r'<br\s*/?>', html_content)
        
        current_name = None
        for part in parts:
            # Nettoyer les balises HTML
            text = BeautifulSoup(part, 'html.parser').get_text()
            text = self.clean_text(text)
            
            if not text:
                continue
            
            # Si c'est une ligne avec "Dénomination en" ou "Langue de la dénomination"
            if 'Dénomination en' in text or 'Langue de la dénomination' in text or 'depuis le' in text.lower():
                if current_name:
                    # Extraire la langue
                    langue = None
                    if 'français' in text:
                        langue = 'français'
                    elif 'néerlandais' in text:
                        langue = 'néerlandais'
                    elif 'non spécifiée' in text:
                        langue = 'non spécifiée'
                    
                    # Extraire la date
                    date = self.extract_date_info(text)
                    
                    denominations.append({
                        'nom': current_name,
                        'langue': langue,
                        'depuis': date
                    })
                    current_name = None
            else:
                # C'est un nom de dénomination ou abréviation
                current_name = text
        
        return denominations if denominations else None
    
    def extract_bce_data(self, html_content):
        """Extrait toutes les données d'une page BCE de manière structurée"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        data = {
            'identification': {},
            'denomination': {},
            'adresse_siege': {},
            'contacts': {},
            'donnees_juridiques': {},
            'organes': [],
            'qualites': [],
            'autorisations': [],
            'activites': {
                'tva_2025': [],
                'tva_2008': [],
                'tva_2003': [],
                'onss_2025': [],
                'onss_2008': []
            },
            'donnees_financieres': {},
            'liens_entites': [],
            'etablissements': {}
        }
        
        tables = soup.find_all('table')
        
        for table in tables:
            rows = table.find_all('tr')
            
            for row in rows:
                cells = row.find_all('td')
                
                if len(cells) == 2:
                    label_raw = cells[0].get_text()
                    label = self.clean_text(label_raw).rstrip(':')
                    value_cell = cells[1]
                    value = self.clean_text(value_cell.get_text())
                    
                    if not label or not value or value == 'Pas de données reprises dans la BCE.' or label == value:
                        continue
                    
                    # Extraction des dates si présentes
                    date_info = self.extract_date_info(value)
                    
                    # IDENTIFICATION
                    if label == "Numéro d'entreprise":
                        data['identification']['numero'] = value
                    elif label == "Statut":
                        data['identification']['statut'] = value.replace('Actif', 'Actif').replace('Arrêté', 'Arrêté')
                    elif label == "Type d'entité":
                        data['identification']['type'] = value
                    
                    # DÉNOMINATION
                    elif label == "Dénomination":
                        parsed = self.parse_denomination_cell(value_cell)
                        if parsed:
                            data['denomination']['noms'] = parsed
                        else:
                            data['denomination']['noms'] = [{'nom': value, 'langue': None, 'depuis': None}]
                    
                    elif label == "Abréviation":
                        parsed = self.parse_denomination_cell(value_cell)
                        if parsed:
                            data['denomination']['abreviations'] = parsed
                        else:
                            data['denomination']['abreviations'] = [{'abréviation': value, 'langue': None, 'depuis': None}]
                    
                    # ADRESSE
                    elif label == "Adresse du siège":
                        adresse_lines = value_cell.get_text().strip().split('\n')
                        rue = self.clean_text(adresse_lines[0]) if len(adresse_lines) > 0 else ''
                        cp_commune = self.clean_text(adresse_lines[1]) if len(adresse_lines) > 1 else ''
                        date = self.extract_date_info(value)
                        data['adresse_siege'] = {
                            'rue': rue,
                            'code_postal_commune': cp_commune,
                            'depuis': date
                        }
                    elif label in ["Rue et numéro", "Code postal", "Commune", "Pays", "Boîte"]:
                        data['adresse_siege'][label.lower().replace(' ', '_')] = value
                    
                    # CONTACTS
                    elif label in ["Téléphone", "Numéro de téléphone", "GSM", "Fax", "Numéro de fax", "E-mail", "Adresse web"]:
                        if value != "Pas de données reprises dans la BCE.":
                            data['contacts'][label.lower().replace(' ', '_').replace('numéro_de_', '')] = value
                    
                    # DONNÉES JURIDIQUES
                    elif label == "Situation juridique":
                        situation = value.split('Depuis')[0].strip()
                        data['donnees_juridiques']['situation'] = situation
                        data['donnees_juridiques']['situation_depuis'] = date_info
                    elif label == "Date de début":
                        data['donnees_juridiques']['date_debut'] = value
                    elif label == "Forme légale":
                        forme = value.split('Depuis')[0].strip()
                        data['donnees_juridiques']['forme_legale'] = forme
                        data['donnees_juridiques']['forme_legale_depuis'] = date_info
                    
                    # ÉTABLISSEMENTS
                    elif label == "Nombre d'unités d'établissement (UE)":
                        nombre = re.search(r'\d+', value)
                        data['etablissements']['nombre'] = int(nombre.group()) if nombre else None
                    
                    # DONNÉES FINANCIÈRES
                    elif label == "Capital":
                        data['donnees_financieres']['capital'] = value
                    elif label == "Assemblée générale":
                        data['donnees_financieres']['assemblee_generale'] = value
                    elif label == "Date de fin de l'année comptable":
                        data['donnees_financieres']['fin_annee_comptable'] = value
                    
                    # CAPACITÉS/QUALITÉS
                    elif 'Dispense' in label:
                        data['qualites'].append({
                            'type': label,
                            'valeur': value.split('Depuis')[0].strip() if 'Depuis' in value else value,
                            'depuis': date_info
                        })
                
                # LIGNES À 3 COLONNES (Organes)
                elif len(cells) == 3:
                    col1 = self.clean_text(cells[0].get_text())
                    col2 = self.clean_text(cells[1].get_text())
                    col3 = self.clean_text(cells[2].get_text())
                    
                    if col1 and col2:
                        if any(term in col1.lower() for term in ['administrateur', 'gérant', 'directeur', 'président', 'liquidateur', 'déléguée', 'délégué']):
                            date = self.extract_date_info(col3) if col3 else None
                            organe = {
                                'fonction': col1,
                                'nom': col2,
                                'depuis': date
                            }
                            if organe not in data['organes']:
                                data['organes'].append(organe)
                
                # LIGNES À 1 COLONNE
                elif len(cells) == 1:
                    text = self.clean_text(cells[0].get_text())
                    if not text or len(text) > 300:
                        continue
                    
                    # QUALITÉS
                    if any(qual in text for qual in ['Assujettie à la TVA', 'Entreprise soumise', 'Pouvoir adjudicateur', 
                                                       'Membre d\'un organe', 'Entreprise commerciale', 'Employeur ONSS']):
                        parts = text.split('Depuis le')
                        qualite_text = parts[0].strip()
                        date = 'Depuis le ' + parts[1].strip() if len(parts) > 1 else None
                        if qualite_text and not any(q['type'] == qualite_text for q in data['qualites']):
                            data['qualites'].append({
                                'type': qualite_text,
                                'depuis': date
                            })
                    
                    # AUTORISATIONS
                    elif 'BELAC' in text or 'Organisme' in text:
                        if text not in data['autorisations']:
                            data['autorisations'].append(text)
                    
                    # ACTIVITÉS TVA/ONSS
                    elif re.search(r'(TVA|ONSS)\s*(2025|2008|2003)', text):
                        match_tva = re.search(r'(TVA|ONSS)\s*(2025|2008|2003)\s*([\d.]+)\s*-\s*(.+?)(?:Depuis le (.+))?$', text)
                        if match_tva:
                            type_act = match_tva.group(1)
                            annee = match_tva.group(2)
                            code = match_tva.group(3)
                            description = match_tva.group(4).strip()
                            date = match_tva.group(5)
                            
                            activite = {
                                'code': code,
                                'description': description,
                                'depuis': date
                            }
                            
                            if type_act == 'TVA':
                                if annee == '2025' and activite not in data['activites']['tva_2025']:
                                    data['activites']['tva_2025'].append(activite)
                                elif annee == '2008' and activite not in data['activites']['tva_2008']:
                                    data['activites']['tva_2008'].append(activite)
                                elif annee == '2003' and activite not in data['activites']['tva_2003']:
                                    data['activites']['tva_2003'].append(activite)
                            elif type_act == 'ONSS':
                                if annee == '2025' and activite not in data['activites']['onss_2025']:
                                    data['activites']['onss_2025'].append(activite)
                                elif annee == '2008' and activite not in data['activites']['onss_2008']:
                                    data['activites']['onss_2008'].append(activite)
                    
                    # LIENS ENTRE ENTITÉS
                    elif re.search(r'\d{4}\.\d{3}\.\d{3}', text) and ('absorbée' in text or 'absorbe' in text):
                        if text not in data['liens_entites']:
                            data['liens_entites'].append(text)
        
        # Nettoyer les sections vides
        data = {k: v for k, v in data.items() if v}
        for key in ['activites']:
            if key in data:
                data[key] = {k: v for k, v in data[key].items() if v}
        
        if 'contacts' in data and not data['contacts']:
            del data['contacts']
        if 'autorisations' in data and not data['autorisations']:
            del data['autorisations']
        if 'activites' in data and not data['activites']:
            del data['activites']
        
        return data
    
    def read_from_hdfs(self, hdfs_path):
        """
        Lit un fichier depuis HDFS.
        
        Args:
            hdfs_path (str): Chemin du fichier dans HDFS
            
        Returns:
            str: Contenu du fichier
        """
        try:
            self.logger.info(f"Lecture du fichier HDFS: {hdfs_path}")
            with self.hdfs_client.read(hdfs_path, encoding='utf-8') as reader:
                content = reader.read()
            self.logger.info(f"Fichier lu avec succès: {len(content)} caractères")
            return content
        except Exception as e:
            self.logger.error(f"Erreur lors de la lecture de {hdfs_path}: {e}")
            raise
    
    def save_to_temp_json(self, data, filename):
        """
        Sauvegarde les données extraites dans un fichier JSON temporaire.
        
        Args:
            data (dict): Données à sauvegarder
            filename (str): Nom du fichier (sans chemin)
            
        Returns:
            str: Chemin complet du fichier sauvegardé
        """
        import os
        
        # Créer le répertoire temporaire s'il n'existe pas
        os.makedirs(self.temp_output_dir, exist_ok=True)
        
        # Générer le chemin complet
        output_path = os.path.join(self.temp_output_dir, filename)
        
        # Sauvegarder
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"Données sauvegardées dans {output_path}")
        return output_path
    
    def process_file(self, input_hdfs_path):
        """
        Traite un fichier HTML BCE depuis HDFS et sauvegarde dans un JSON temporaire.
        
        Args:
            input_hdfs_path (str): Chemin du fichier HTML d'entrée dans HDFS
        
        Returns:
            tuple: (données extraites, chemin du fichier JSON temporaire)
        """
        try:
            # Lire le fichier HTML depuis HDFS
            html_content = self.read_from_hdfs(input_hdfs_path)
            
            # Extraire les données
            self.logger.info(f"Extraction des données de {input_hdfs_path}")
            extracted_data = self.extract_bce_data(html_content)
            
            # Ajouter des métadonnées
            from datetime import datetime
            extracted_data['_metadata'] = {
                'source_file': input_hdfs_path,
                'extraction_date': datetime.now().isoformat(),
                'extractor_version': '1.0'
            }
            
            # Déterminer le nom du fichier de sortie
            filename = input_hdfs_path.split('/')[-1].replace('.htm', '').replace('.html', '')
            output_filename = f"{filename}_extracted.json"
            
            # Sauvegarder dans un fichier temporaire
            output_path = self.save_to_temp_json(extracted_data, output_filename)
            
            self.logger.info(f"Traitement terminé avec succès pour {input_hdfs_path}")
            return extracted_data, output_path
            
        except Exception as e:
            self.logger.error(f"Erreur lors du traitement de {input_hdfs_path}: {e}")
            raise
        
    def process_multiple_files(self, input_hdfs_paths):
        """
        Traite plusieurs fichiers HTML BCE depuis HDFS.
        
        Args:
            input_hdfs_paths (list): Liste des chemins des fichiers HTML dans HDFS
        
        Returns:
            list: Liste de tuples (données, chemin_json_temp)
        """
        results = []
        
        for hdfs_path in input_hdfs_paths:
            try:
                data, json_path = self.process_file(hdfs_path)
                results.append((data, json_path))
                self.logger.info(f"Fichier {hdfs_path} traité avec succès")
            except Exception as e:
                self.logger.error(f"Erreur lors du traitement de {hdfs_path}: {e}")
                # Continuer avec les autres fichiers
                continue
        
        return results