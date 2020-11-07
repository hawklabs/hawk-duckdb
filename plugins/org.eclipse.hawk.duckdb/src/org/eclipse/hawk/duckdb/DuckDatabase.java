package org.eclipse.hawk.duckdb;

import java.io.File;
import java.util.Map;
import java.util.Set;

import org.eclipse.hawk.core.IConsole;
import org.eclipse.hawk.core.graph.IGraphDatabase;
import org.eclipse.hawk.core.graph.IGraphEdge;
import org.eclipse.hawk.core.graph.IGraphIterable;
import org.eclipse.hawk.core.graph.IGraphNode;
import org.eclipse.hawk.core.graph.IGraphNodeIndex;
import org.eclipse.hawk.core.graph.IGraphTransaction;

public class DuckDatabase implements IGraphDatabase {

	@Override
	public String getHumanReadableName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getPath() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void run(File parentfolder, IConsole c) {
		// TODO Auto-generated method stub

	}

	@Override
	public void shutdown() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public IGraphNodeIndex getOrCreateNodeIndex(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IGraphNodeIndex getMetamodelIndex() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IGraphNodeIndex getFileIndex() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IGraphTransaction beginTransaction() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isTransactional() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void enterBatchMode() {
		// TODO Auto-generated method stub

	}

	@Override
	public void exitBatchMode() {
		// TODO Auto-generated method stub

	}

	@Override
	public IGraphIterable<? extends IGraphNode> allNodes(String label) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IGraphNode createNode(Map<String, Object> props, String label) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IGraphEdge createRelationship(IGraphNode start, IGraphNode end, String type) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IGraphEdge createRelationship(IGraphNode start, IGraphNode end, String type, Map<String, Object> props) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getGraph() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IGraphNode getNodeById(Object id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean nodeIndexExists(String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getTempDir() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Mode currentMode() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> getNodeIndexNames() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> getKnownMMUris() {
		// TODO Auto-generated method stub
		return null;
	}

}
