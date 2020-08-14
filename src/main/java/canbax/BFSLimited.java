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
     * @param ids
     * @param depthLimit
     * @return
     */
    @Procedure(value = "BFS", mode = Mode.WRITE)
    @Description("finds the minimal sub-graph from given nodes")
    public Stream<Output> BFS(@Name("ids") List<Long> ids, @Name("depthLimit") long depthLimit) {
        Output o = new Output();

        // always return the source nodes
        for (long id : ids) {
            o.nodes.add(this.db.getNodeById(id));
        }

        Queue<Long> queue = new LinkedList<>(ids);
        HashSet<Long> visitedNodes = new HashSet<>(ids);

        while (!queue.isEmpty()) {
            Node n1 = this.db.getNodeById(queue.remove());

            Iterable<Relationship> edges = n1.getRelationships();
            for (Relationship e : edges) {
                Node n2 = e.getOtherNode(n1);
                if (visitedNodes.contains(n2.getId())) {
                    continue;
                }
                visitedNodes.add(n1.getId());
                queue.add(n2.getId());
                o.edges.add(e);
                o.nodes.add(n2);
            }
        }


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
