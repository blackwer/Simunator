import itertools as it
import numpy as np


class ParamMaker:
    def __init__(self, indist: dict = {}):
        """

        """
        if indist:
            self._disttype = list(indist.keys())[0]
            self._params = indist[self._disttype]["params"]
            self._samples = indist[self._disttype]["samples"]
            self.build_generator()

    @classmethod
    def from_param_makers(self, *args):
        self._disttype = "Combo"
        self._params = self.flatten([pm._params for pm in args])
        self._generator = it.product(*[pm._generator for pm in args])
        return self

    def build_generator(self):
        if self._disttype == "RandUniform":
            self._generator = self.rand_uniform(
                [val["bounds"] for param, val in self._params.items()], self._samples
            )
        elif self._disttype == "Uniform":
            self._generator = self.lin_uniform(
                [val["bounds"] for param, val in self._params.items()], self._samples
            )

    def rand_uniform(self, bounds, N):
        for i in range(0, N):
            yield [np.random.uniform(*bound) for bound in bounds]

    def lin_uniform(self, bounds, N):
        for i in range(0, N):
            point = []
            for bound in bounds:
                slope = 0 if N == 1 else (bound[1] - bound[0]) / (N - 1)
                point.append(bound[0] + slope * i)
            yield point

    @staticmethod
    def flatten(l):
        return [item for sublist in l for item in sublist]

    def items(self):
        return self._generator
