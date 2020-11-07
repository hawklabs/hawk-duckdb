/*******************************************************************************
 * Copyright (c) 2015-2020 The University of York, Aston University.
 * 
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 3.
 *
 * SPDX-License-Identifier: EPL-2.0 OR GPL-3.0
 *
 * Contributors:
 *     Antonio Garcia-Dominguez - initial API and implementation
 ******************************************************************************/
package org.eclipse.hawk.duckdb.tests;

import org.eclipse.hawk.backend.tests.factories.IGraphDatabaseFactory;
import org.eclipse.hawk.core.graph.IGraphDatabase;
import org.eclipse.hawk.duckdb.DuckDatabase;

public final class DuckDatabaseFactory implements IGraphDatabaseFactory {
	@Override
	public IGraphDatabase create() {
		// Use a shorter cache expiration period to trigger any caching issues ASAP
		DuckDatabase db = new DuckDatabase();
		return db;
	}

	@Override
	public String toString() {
		return "DuckDB";
	}
}