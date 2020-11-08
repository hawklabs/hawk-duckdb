/*******************************************************************************
 * Copyright (c) 2020 The University of York, Aston University.
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

import java.util.Collections;

import org.eclipse.hawk.backend.tests.IndexTest;
import org.eclipse.hawk.backend.tests.factories.IGraphDatabaseFactory;
import org.junit.runners.Parameterized.Parameters;

public class DuckBackendTestSuite extends IndexTest {

	public DuckBackendTestSuite(IGraphDatabaseFactory dbFactory) {
		super(dbFactory);
	}

//	@Parameters(name="{0}")
//	public static Object[][] params() {
//		return new Object[][] {
//			{ new DuckDatabaseFactory() },
//		};
//	}

	@Parameters(name="{0}")
	public static Iterable<Object[]> params() {
		return Collections.singletonList( new Object[] {
			new DuckDatabaseFactory()
		});
	}
	
}
