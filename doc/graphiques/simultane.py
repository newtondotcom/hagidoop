import matplotlib.pyplot as plt
import numpy as np

# Données
nbMachine = [2, 3, 5]
times = [7, 10.277, 5.99]

times2 = [5.537, 9.18, 13.270, 17.118]
tailleFichier = [2.7, 5.5, 8, 11]

times3 = [7.412, 7.489, 7.565, 8.023]
tailleFragment = [100000000, 50000000, 25000000, 10000000]
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
ax.set_title("Temps d'exécution en fonction de la taille des fichiers \n avec 2 machines et des fragments de 100 000 000 de caractère")
ax.plot(tailleFichier, times2)
ax.legend()
plt.savefig("images/TempssurTaille.png")
plt.show()
plt.close()
fig, ax = plt.subplots()
ax.set_xlabel("Taille des fragment")
ax.set_ylabel("Temps d'exécution")
ax.set_title("Temps d'exécution en fonction de la taille des fragments \n avec 2 machines et un fichiers de 2,5GB")
ax.plot(tailleFragment, times3)
ax.legend()
plt.savefig("images/TempsSurFragment.png")
plt.show()
plt.close()
