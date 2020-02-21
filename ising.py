import numpy as np
import scipy.integrate as integrate

J = {J}
k = 1.0 / np.sinh(2 * J) ** 2
Tc = 2 * J / k / np.log(1 + np.sqrt(2))
T = 1
integrand = lambda theta: 1.0 / np.sqrt(1 - 4 * k * np.sin(theta) ** 2 / (1 + k) ** 2)
U = (
    -J
    / np.tanh(2 * J)
    * (
        1
        + 2
        / np.pi
        * (2 * np.tanh(2 * J) ** 2 - 1)
        * integrate.quad(integrand, 0.0, np.pi / 2)[0]
    )
)
M = 0 if T > Tc else (1 - 1.0 / np.sinh(2 * J) ** 4) ** (1.0 / 8.0)

print(J, Tc, U, M)

with open('M.out', 'w') as f:
    f.write(str(M))
