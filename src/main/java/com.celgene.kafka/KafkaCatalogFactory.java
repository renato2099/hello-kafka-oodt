package com.celgene.kafka;

import org.apache.oodt.cas.filemgr.catalog.Catalog;
import org.apache.oodt.cas.filemgr.catalog.SpacerCatalogFactory;

public class KafkaCatalogFactory extends SpacerCatalogFactory {

    @Override
    protected  Catalog getWrapper(Catalog sub) {
        return new SampleKafkaCatalog(sub);
    }
}
