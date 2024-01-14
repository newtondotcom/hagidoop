import matplotlib.pyplot as plt
import numpy as np

# Données
xd = [1, 2, 3, 4, 5]
times = [10, 7, 5, 3, 2]
         
# Graphique
fig, ax = plt.subplots()
ax.set_xlabel("Nombres de machines")
ax.set_ylabel("Temps d'exécution")
ax.set_title("Temps d'exécution pour un fichier de 10Go")
ax.plot(xd, times)
ax.legend()
plt.savefig("images/simultane10go.png")
plt.show()

