package com.thinkaurelius.titan.tinkerpop.gremlin;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Text;
import com.tinkerpop.blueprints.Query;

import java.util.ArrayList;
import java.util.List;

/**
 * Titan specific Gremlin imports
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Imports {

    private static final List<String> imports = new ArrayList<String>();

    static {
        // titan
        imports.add("com.thinkaurelius.titan.core.*");
        imports.add("com.thinkaurelius.titan.core.attribute.*");
        imports.add("com.thinkaurelius.titan.core.log.*");
        imports.add("com.thinkaurelius.titan.core.olap.*");
        imports.add("com.thinkaurelius.titan.core.schema.*");
        imports.add("com.thinkaurelius.titan.core.util.*");
        imports.add("com.thinkaurelius.titan.example.*");
        imports.add("org.apache.commons.configuration.*");
        imports.add("static " + Geo.class.getName() + ".*");
        imports.add("static " + Text.class.getName() + ".*");
        imports.add("static " + Cardinality.class.getName() + ".*");
        imports.add("static " + Multiplicity.class.getName() + ".*");
        // todo: remove with Gremlin 2.3.1+
        imports.add("static " + Query.Compare.class.getName() + ".*");
    }

    public static List<String> getImports() {
        return Imports.imports;
    }
}
