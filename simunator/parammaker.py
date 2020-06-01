import itertools as it
import numpy as np


class ParamMaker:
    def __init__(self, indist: dict = {}):
        """

        """
        if indist:
            self._disttype = list(indist.keys())[0]
            self._params = indist[self._disttype]["params"]
            if "samples" in indist[self._disttype]:
                self._samples = indist[self._disttype]["samples"]
            self.build_generator()

    @classmethod
    def from_param_makers(self, *args):
        self._disttype = "Combo"
        self._params = self.flatten([pm._params for pm in args])
        self._generator = it.product(*[pm._generator for pm in args])
        return self

    def actualize(self):
        return self._params, [tuple(self.flatten(tup))
                              for tup in self.items()]

    def build_generator(self):
        if self._disttype == "RandUniform":
            self._generator = self.rand_uniform(
                [val["bounds"]
                    for _, val in self._params.items()], self._samples
            )
        elif self._disttype == "Uniform":
            self._generator = self.lin_uniform(
                [val["bounds"]
                    for _, val in self._params.items()], self._samples
            )
        elif self._disttype == "ItemizedList":
            param = list(self._params.keys())[0]
            self._generator = ([el] for el in self._params[param])
        elif self._disttype == "Halton":
            param = list(self._params.keys())[0]
            self._generator = self.halton(
                [val["bounds"]
                    for _, val in self._params.items()], self._samples)

    def halton(self, bounds, N):
        import chaospy
        dim = len(self._params)
        unscaled = chaospy.distributions.sampler.sequences.halton.create_halton_samples(
            N, dim)
        for i in range(0, N):
            yield [bounds[j][0] + (bounds[j][1] - bounds[j][0])*unscaled[j][i] for j in range(dim)]

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

    def itemized_list(self, inlist):
        return (el for el in inlist)

    @staticmethod
    def flatten(l):
        return [item for sublist in l for item in sublist]

    def items(self):
        return self._generator
