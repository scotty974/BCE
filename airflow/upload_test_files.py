import subprocess
import os

def run_command(command):
    """Execute une commande et affiche le résultat"""
    try:
        result = subprocess.run(command, shell=True, check=True, 
                              capture_output=True, text=True)
        if result.stdout:
            print(result.stdout.strip())
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Erreur: {e.stderr}")
        return False

def upload_files():
    test_files = [
        ('Page1.htm', '0203430576.htm'),
        ('Page2.htm', '0260581887.htm'),
        ('Page3.htm', '0414042619.htm'),
    ]
    
    # Vérifier que les fichiers existent
    for local_file, _ in test_files:
        if not os.path.exists(local_file):
            print(f"✗ Fichier {local_file} introuvable")
            return
    
    print("📁 Création du répertoire dans HDFS...")
    run_command("docker exec namenode_exo_bce hdfs dfs -mkdir -p /bce_data")
    
    print("\n📤 Upload des fichiers...")
    for local_file, hdfs_name in test_files:
        # Copier dans le conteneur
        print(f"  Copie de {local_file} -> conteneur...")
        if not run_command(f"docker cp {local_file} namenode_exo_bce:/tmp/{hdfs_name}"):
            continue
        
        # Mettre dans HDFS
        print(f"  Upload vers HDFS: /bce_data/{hdfs_name}")
        if run_command(f"docker exec namenode_exo_bce hdfs dfs -put -f /tmp/{hdfs_name} /bce_data/"):
            print(f"  ✓ {hdfs_name} uploadé avec succès")
        
        # Nettoyer le fichier temporaire dans le conteneur
        run_command(f"docker exec namenode_exo_bce rm /tmp/{hdfs_name}")
    
    print("\n📂 Contenu de /bce_data dans HDFS:")
    run_command("docker exec namenode_exo_bce hdfs dfs -ls /bce_data/")
    
    print("\n✅ Upload terminé!")

if __name__ == '__main__':
    upload_files()