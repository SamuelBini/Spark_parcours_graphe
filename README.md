# Parcours en largeur de graphes avec spark

Ce fichier est à exécuter sur la machine virtuelle

Vous devez suivre les étapes suivantes : 
    
-   Mettre le fichier à traiter sur HDFS
        
    `hadoop fs -put graphe.txt /`

    
-   Exécuter le script
        
    `spark-submit --master "local[2]" FICHIER.py`
