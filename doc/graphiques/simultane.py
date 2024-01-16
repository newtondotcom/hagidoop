import matplotlib.pyplot as plt
import numpy as np

# Données
nbMachine = [1, 2, 3, 4, 5]
times = [10, 7, 5, 3, 2]

times2 = [5.379, ]
tailleFichier = [2.7, ]
# Graphique
fig, ax = plt.subplots()
ax.set_xlabel("Nombres de machines")
ax.set_ylabel("Temps d'exécution")
ax.set_title("Temps d'exécution pour un fichier de 10Go")
ax.plot(nbMachine, times)
ax.legend()
plt.savefig("images/simultane10go.png")
plt.show()
plt.close()
fig, ax = plt.subplots()
ax.set_xlabel("Taille du fichier")
ax.set_ylabel("Temps d'exécution")
ax.set_title("Temps d'exécution en fonction de la taille des fichiers avec 2 machines et taille de fragment = 128 000 000")
ax.plot(tailleFichier, times2)
ax.legend()
plt.savefig("images/TempssurTaille.png")
plt.show()

