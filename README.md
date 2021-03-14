# Brief_Foil


## Contexte du projet

Après avoir vu les images de l’America’s cup dans la baie d’Auckland, Cléante de Nantes a décidé de se mettre au catamaran avec foil.

Avant d’investir dans son engin high tech, elle a décidé de faire une petite application pour sa montre connectée qui donne en temps réel les dernières mesures sur 3 stations locales disposant d’un anémomètre. Elle souhaite savoir où se diriger pour trouver le vent nécessaire à ses futures activités.

Malheureusement sa montre dispose d’un stockage particulièrement réduit, donc la base de données locale devra être mise à jour régulièrement tout en s’assurant de ne pas dépasser un volume de stockage correspondant aux 10 derniers enregistrements. Enfin, un tantinet paranoïaque, elle souhaite absolument avoir la dernière sauvegarde qui devra être mise à jour régulièrement (une fois tous les 5 enregistrements).

Pourrez-vous aider Cléante à réaliser son projet en moins de 48h chrono?

Contrainte: usage de SQLite sur la montre

​

Les données pourront être récupérées auprès d’anémomètres connectés: les “pioupiou”s https://www.openwindmap.org/PP113

Ceux-ci disposent d’une API qui peut être interrogée: http://developers.pioupiou.fr/api/live/


