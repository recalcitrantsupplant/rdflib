from __future__ import annotations

import pathlib
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    BinaryIO,
    TextIO,
)

from rdflib import plugin
from rdflib.namespace import NamespaceManager
from rdflib.parser import InputSource
from rdflib.store import Store
from rdflib.term import (
    BNode,
    Literal,
    URIRef, IdentifiedNode,
)

if TYPE_CHECKING:
    from collections.abc import Generator

from rdflib.graph import _ContextIdentifierType, _TripleType, \
    _OptionalIdentifiedQuadType, _TripleOrQuadPatternType, _DatasetT, _ContextType, \
    Graph, _TripleSliceType, _QuadSliceType

from rdflib._type_checking import _NamespaceSetString


class Dataset():
    """
    An RDFLib Dataset is an object that stores multiple Named Graphs - instances of
    RDFLib Graph identified by IRI - within it and allows whole-of-dataset or single
    Graph use.

    RDFLib's Dataset class is based on the `RDF 1.2. 'Dataset' definition
    <https://www.w3.org/TR/rdf12-datasets/>`_:

        An RDF dataset is a collection of RDF graphs, and comprises:

        - Exactly one default graph, being an RDF graph. The default graph does not
            have a name and MAY be empty.
        - Zero or more named graphs. Each named graph is a pair consisting of an IRI or
            a blank node (the graph name), and an RDF graph. Graph names are unique
            within an RDF dataset.

    Accordingly, a Dataset allows for `Graph` objects to be added to it with
    :class:`rdflib.term.URIRef` or :class:`rdflib.term.BNode` identifiers and always
    creats a default graph with the :class:`rdflib.term.URIRef` identifier
    :code:`urn:x-rdflib:default`.

    Dataset extends Graph's Subject, Predicate, Object (s, p, o) 'triple'
    structure to include a graph identifier - archaically called Context - producing
    'quads' of s, p, o, g.

    Triples, or quads, can be added to a Dataset. Triples, or quads with the graph
    identifer :code:`urn:x-rdflib:default` go into the default graph.

    .. note:: Dataset builds on the `ConjunctiveGraph` class but that class's direct
        use is now deprecated (since RDFLib 7.x) and it should not be used.
        `ConjunctiveGraph` will be removed from future RDFLib versions.

    Examples of usage and see also the examples/datast.py file:

    >>> # Create a new Dataset
    >>> ds = Dataset()
    >>> # simple triples goes to default graph
    >>> ds.add((
    ...     URIRef("http://example.org/a"),
    ...     URIRef("http://www.example.org/b"),
    ...     Literal("foo")
    ... ))  # doctest: +ELLIPSIS
    <Graph identifier=... (<class 'rdflib.graph.Dataset'>)>
    >>>
    >>> # Create a graph in the dataset, if the graph name has already been
    >>> # used, the corresponding graph will be returned
    >>> # (ie, the Dataset keeps track of the constituent graphs)
    >>> g_id = URIRef("http://www.example.com/gr")
    >>> g = Graph()
    >>>
    >>> # add triples to the new graph as usual
    >>> g.add_named_graph(g, g_id) # doctest: +ELLIPSIS
    <Graph identifier=... (<class 'rdflib.graph.Graph'>)>
    >>> # alternatively: add a quad to the dataset -> goes to the graph
    >>> ds.add((
    ...     URIRef("http://example.org/x"),
    ...     URIRef("http://example.org/z"),
    ...     Literal("foo-bar"),
    ...     g_id
    ... )) # doctest: +ELLIPSIS
    <Graph identifier=... (<class 'rdflib.graph.Dataset'>)>
    >>>
    >>> # querying triples return them all regardless of the graph
    >>> for t in ds.triples((None,None,None)):  # doctest: +SKIP
    ...     print(t)  # doctest: +NORMALIZE_WHITESPACE
    (rdflib.term.URIRef("http://example.org/a"),
     rdflib.term.URIRef("http://www.example.org/b"),
     rdflib.term.Literal("foo"))
    (rdflib.term.URIRef("http://example.org/x"),
     rdflib.term.URIRef("http://example.org/z"),
     rdflib.term.Literal("foo-bar"))
    (rdflib.term.URIRef("http://example.org/x"),
     rdflib.term.URIRef("http://example.org/y"),
     rdflib.term.Literal("bar"))
    >>>
    >>> # querying quads() return quads; the fourth argument can be unrestricted
    >>> # (None) or restricted to a graph
    >>> for q in ds.quads((None, None, None, None)):  # doctest: +SKIP
    ...     print(q)  # doctest: +NORMALIZE_WHITESPACE
    (rdflib.term.URIRef("http://example.org/a"),
     rdflib.term.URIRef("http://www.example.org/b"),
     rdflib.term.Literal("foo"),
     None)
    (rdflib.term.URIRef("http://example.org/x"),
     rdflib.term.URIRef("http://example.org/y"),
     rdflib.term.Literal("bar"),
     rdflib.term.URIRef("http://www.example.com/gr"))
    (rdflib.term.URIRef("http://example.org/x"),
     rdflib.term.URIRef("http://example.org/z"),
     rdflib.term.Literal("foo-bar"),
     rdflib.term.URIRef("http://www.example.com/gr"))
    >>>
    >>> # unrestricted looping is equivalent to iterating over the entire Dataset
    >>> for q in ds:  # doctest: +SKIP
    ...     print(q)  # doctest: +NORMALIZE_WHITESPACE
    (rdflib.term.URIRef("http://example.org/a"),
     rdflib.term.URIRef("http://www.example.org/b"),
     rdflib.term.Literal("foo"),
     None)
    (rdflib.term.URIRef("http://example.org/x"),
     rdflib.term.URIRef("http://example.org/y"),
     rdflib.term.Literal("bar"),
     rdflib.term.URIRef("http://www.example.com/gr"))
    (rdflib.term.URIRef("http://example.org/x"),
     rdflib.term.URIRef("http://example.org/z"),
     rdflib.term.Literal("foo-bar"),
     rdflib.term.URIRef("http://www.example.com/gr"))
    >>>
    >>> # restricting iteration to a graph:
    >>> for q in ds.quads((None, None, None, g)):  # doctest: +SKIP
    ...     print(q)  # doctest: +NORMALIZE_WHITESPACE
    (rdflib.term.URIRef("http://example.org/x"),
     rdflib.term.URIRef("http://example.org/y"),
     rdflib.term.Literal("bar"),
     rdflib.term.URIRef("http://www.example.com/gr"))
    (rdflib.term.URIRef("http://example.org/x"),
     rdflib.term.URIRef("http://example.org/z"),
     rdflib.term.Literal("foo-bar"),
     rdflib.term.URIRef("http://www.example.com/gr"))
    >>> # Note that in the call above -
    >>> # ds.quads((None,None,None,"http://www.example.com/gr"))
    >>> # would have been accepted, too
    >>>
    >>> # graph names in the dataset can be queried:
    >>> for c in ds.graphs():  # doctest: +SKIP
    ...     print(c.identifier)  # doctest:
    urn:x-rdflib:default
    http://www.example.com/gr
    >>> # A graph can be created without specifying a name; a skolemized genid
    >>> # is created on the fly
    >>> g2 = Graph()
    >>> ds.add_graph(g2)
    >>> for c in ds.graphs():  # doctest: +SKIP
    ...     print(c)  # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    https://rdflib.github.io/.well-known/genid/rdflib/N...
    http://www.example.com/gr
    >>> # Note that the Dataset.graphs() call returns names of empty graphs,
    >>> # too. This can be restricted:
    >>> for c in ds.graphs(empty=False):  # doctest: +SKIP
    ...     print(c)  # doctest: +NORMALIZE_WHITESPACE
    http://www.example.com/gr
    >>>
    >>> # a graph can also be removed from a dataset via ds.remove_graph(g)

    ... versionadded:: 4.0
    """

    def __init__(
        self,
        store: Store | str = "default",
        namespace_manager: NamespaceManager | None = None,
        base: str | None = None,
        bind_namespaces: _NamespaceSetString = "rdflib",
    ):
        super(Dataset, self).__init__(store=store, identifier=None)
        self.base = base
        self.__store: Store
        if not isinstance(store, Store):
            # TODO: error handling
            self.__store = store = plugin.get(store, Store)()
        else:
            self.__store = store
        self.__namespace_manager = namespace_manager
        self._bind_namespaces = bind_namespaces
        self.context_aware = True
        self.formula_aware = False

        if not self.store.graph_aware:
            raise Exception("Dataset must be backed by a graph-aware store!")

    def __getnewargs__(self) -> tuple[Any, ...]:
        return (self.store, self.default_union, self.default_graph.base)

    def __str__(self) -> str:
        pattern = (
            "[a rdflib:Dataset;rdflib:storage " "[a rdflib:Store;rdfs:label '%s']]"
        )
        return pattern % self.store.__class__.__name__

    # type error: Return type "tuple[Type[Dataset], tuple[Store, bool]]" of "__reduce__" incompatible with return type "tuple[Type[Graph], tuple[Store, IdentifiedNode]]" in supertype "ConjunctiveGraph"
    # type error: Return type "tuple[Type[Dataset], tuple[Store, bool]]" of "__reduce__" incompatible with return type "tuple[Type[Graph], tuple[Store, IdentifiedNode]]" in supertype "Graph"
    def __reduce__(self) -> tuple[type[Dataset], tuple[Store, bool]]:  # type: ignore[override]
        return type(self), (self.store, self.default_union)

    def __getstate__(self) -> tuple[Store, _ContextIdentifierType, _ContextType, bool]:
        return self.store, self.identifier, self.default_graph, self.default_union

    def __setstate__(
        self, state: tuple[Store, _ContextIdentifierType, _ContextType, bool]
    ) -> None:
        # type error: Property "store" defined in "Graph" is read-only
        # type error: Property "identifier" defined in "Graph" is read-only
        self.store, self.identifier, self.default_graph, self.default_union = state  # type: ignore[misc]

    def add_named_graph(
        self,
        graph: Graph,
        identifier: _ContextIdentifierType | _ContextType | str | None = None,
        base: str | None = None,
    ) -> Graph:
        if identifier is None:
            from rdflib.term import _SKOLEM_DEFAULT_AUTHORITY, rdflib_skolem_genid

            self.bind(
                "genid",
                _SKOLEM_DEFAULT_AUTHORITY + rdflib_skolem_genid,
                override=False,
            )
            identifier = BNode().skolemize()

        graph.base = base

        self.store.add_graph(graph, identifier)
        return graph

    def has_named_graph(self, identifier: _ContextIdentifierType) -> bool:
        raise NotImplementedError

    def remove_named_graph(self, identifier: _ContextIdentifierType):
        raise NotImplementedError

    def get_named_graph(self, identifier: _ContextIdentifierType) -> Graph:
        raise NotImplementedError

    def replace_named_graph(self, identifier: _ContextIdentifierType, graph: Graph):
        raise NotImplementedError

    def parse(
        self,
        source: (
            IO[bytes] | TextIO | InputSource | str | bytes | pathlib.PurePath | None
        ) = None,
        publicID: str | None = None,  # noqa: N803
        format: str | None = None,
        location: str | None = None,
        file: BinaryIO | TextIO | None = None,
        data: str | bytes | None = None,
        **args: Any,
    ) -> Graph:
        raise NotImplementedError

    def graphs(
        self, triple: _TripleType | None = None
    ) -> Generator[tuple[IdentifiedNode, Graph], None, None]:
        raise NotImplementedError

    def triples(
        self, triple: _TripleSliceType = None):
        raise NotImplementedError

    # type error: Return type "Generator[tuple[Node, Node, Node, Optional[Node]], None, None]" of "quads" incompatible with return type "Generator[tuple[Node, Node, Node, Optional[Graph]], None, None]" in supertype "ConjunctiveGraph"
    def quads(  # type: ignore[override]
        self, quad: _QuadSliceType = None
    ) -> Generator[_OptionalIdentifiedQuadType, None, None]:
        raise NotImplementedError

    def default_graph(self) -> Graph:
        return self.default_graph

    # type error: Return type "Generator[tuple[Node, URIRef, Node, Optional[IdentifiedNode]], None, None]" of "__iter__" incompatible with return type "Generator[tuple[IdentifiedNode, IdentifiedNode, Union[IdentifiedNode, Literal]], None, None]" in supertype "Graph"
    def __iter__(  # type: ignore[override]
        self,
    ) -> Generator[_OptionalIdentifiedQuadType, None, None]:
        """Iterates over all quads in the store"""
        return self.quads((None, None, None, None))
