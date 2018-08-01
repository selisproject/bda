package gr.ntua.ece.cslab.selis.bda.common.storage;

import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class AbstractTestConnector {
    private final static Logger LOGGER = Logger.getLogger(AbstractTestConnector.class.getCanonicalName());

    public AbstractTestConnector(){}

    public void setUp() {
        SystemConnector.init("../conf/bdatest.properties");
    }

    public void tearDown(){
        SystemConnector.getInstance().close();
    }
}
