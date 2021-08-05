package pl.touk.nifi.ignite;

import org.apache.ignite.Ignite;
import org.apache.nifi.controller.ControllerService;

public interface IgniteThickClientService extends ControllerService {

    Ignite getIgnite();
}
