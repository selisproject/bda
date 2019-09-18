/*
 * Copyright 2019 ICCS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gr.ntua.ece.cslab.selis.bda.controller.cron;

import gr.ntua.ece.cslab.selis.bda.analyticsml.RunnerInstance;
import gr.ntua.ece.cslab.selis.bda.controller.resources.JobResource;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Job;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ScheduledTask extends TimerTask {
    private final static Logger LOGGER = Logger.getLogger(JobResource.class.getCanonicalName());

    private int jobId;
    private String scn_slug;

    public ScheduledTask(String scn_slug, Job job) {
        this.jobId = job.getId();
        this.scn_slug = scn_slug;
    }


    @Override
    public void run() {
        LOGGER.log(Level.INFO, "Cron Task for job " + this.jobId + " launched at " + new Date());

        try {
            (new RunnerInstance(this.scn_slug, this.jobId)).run("");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
