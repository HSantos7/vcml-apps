package edu.msu.cse.cops.server;

import edu.msu.cse.cops.metadata.Dependency;
import edu.msu.cse.cops.metadata.Record;

import java.util.List;

public class RecordDependencies {
    Record record;
    List<Dependency> deps;

    public RecordDependencies(Record rec, List<Dependency> deps) {
        this.record = rec;
        this.deps = deps;
    }

    public Record getRecord() {
        return record;
    }

    public void setRecord(Record record) {
        this.record = record;
    }

    public List<Dependency> getDeps() {
        return deps;
    }

    public void setDeps(List<Dependency> deps) {
        this.deps = deps;
    }
}
