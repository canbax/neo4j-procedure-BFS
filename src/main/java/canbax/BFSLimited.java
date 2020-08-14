package canbax;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Stream;

public class BFSLimited {

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    /**
     *
     * @param ids
     * @param lengthLimit
     * @param isDirected
     * @return
     */
    @Procedure(value = "BFS", mode = Mode.WRITE)
    @Description("finds the minimal sub-graph from given nodes")
    public Stream<Output> BFS(@Name("ids") List<Long> ids, @Name("lengthLimit") long lengthLimit, @Name("isDirected") boolean isDirected) {
        Output o = new Output();
        return Stream.of(o);
    }

    public static class Output {
        public List<Node> nodes;
        public List<Relationship> edges;

        Output() {
            this.nodes = new ArrayList<>();
            this.edges = new ArrayList<>();
        }
    }
}
