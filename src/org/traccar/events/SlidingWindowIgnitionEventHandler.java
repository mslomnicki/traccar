package org.traccar.events;

import lombok.extern.slf4j.Slf4j;
import org.traccar.BaseEventHandler;
import org.traccar.Context;
import org.traccar.model.Device;
import org.traccar.model.DeviceState;
import org.traccar.model.Event;
import org.traccar.model.Position;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SlidingWindowIgnitionEventHandler extends BaseEventHandler {
    private static final Map<Long, List<Position>> RX_FLOWS = new HashMap<>();

    public static final double MAX_SPEED = 5;
    public static final long MINUTES = 15;
    public static final String ATTRIBUTE_PERCENT = "ignitionDuringPausePercent";

    @Override
    protected Map<Event, Position> analyzePosition(Position position) {
        Long devId = position.getDeviceId();
        Device device = Context.getIdentityManager().getById(devId);
        if (device == null
                || !Context.getIdentityManager().isLatestPosition(position)
                || !position.getAttributes().containsKey(Position.KEY_IGNITION)) {
            return null;
        }
        double percent = Context.getDeviceManager().lookupAttributeDouble(devId, ATTRIBUTE_PERCENT, 0, false) / 100;
        if (percent == 0) {
            return null;
        }

        DeviceState deviceState = Context.getDeviceManager().getDeviceState(devId);

        List<Position> positionList = initializeList(devId);
        int listSizeBefore = positionList.size();
        cleanupListAndAddCurrentPosition(positionList, position);
        long timeIgnitionOn = calculateTimeWithIgnitionOnDuringPause(positionList);
        boolean isTimeWithIgnitionOnExceeded = timeIgnitionOn > percent * TimeUnit.MINUTES.toMillis(MINUTES);
        log.info(device.getName()
                + " - before: " + listSizeBefore
                + ", after: " + positionList.size()
                + ", ignOn: " + TimeUnit.MILLISECONDS.toSeconds(timeIgnitionOn)
                + ", perc: " + timeIgnitionOn / TimeUnit.MINUTES.toMillis(MINUTES) * 100
                + ", thr: " + TimeUnit.MINUTES.toSeconds(MINUTES) * percent
                + ", bool: " + isTimeWithIgnitionOnExceeded);

        Map<Event, Position> result = null;
        if (isTimeWithIgnitionOnExceeded && !deviceState.getIgnitionDuringPauseState()) {
            log.info(device.getName() + " - fire event!");
            result = Collections.singletonMap(
                    new Event(Event.TYPE_IGNITION_DURING_PAUSE, position.getDeviceId(), position.getId()), position);
        }
        deviceState.setIgnitionDuringPauseState(isTimeWithIgnitionOnExceeded);
        Context.getDeviceManager().setDeviceState(devId, deviceState);
        return result;
    }

    private List<Position> initializeList(Long devId) {
        List<Position> list;
        synchronized (RX_FLOWS) {
            list = RX_FLOWS.get(devId);
            if (list == null) {
                log.info("New list for device " + devId);
                list = new LinkedList<>();
                RX_FLOWS.put(devId, list);
            }
        }
        return list;
    }

    private synchronized void cleanupListAndAddCurrentPosition(List<Position> positionList, Position currentPosition) {
        long currentTime = currentPosition.getFixTime().getTime();
        long thresholdTime = currentTime - TimeUnit.MINUTES.toMillis(MINUTES);

        Iterator<Position> iterator = positionList.iterator();
        while (iterator.hasNext()) {
            Position pos = iterator.next();
            if (pos.getFixTime().getTime() < thresholdTime) {
                log.debug("Removed position with time " + pos.getFixTime());
                iterator.remove();
            }
        }
        positionList.add(currentPosition);
    }

    private long calculateTimeWithIgnitionOnDuringPause(List<Position> positions) {
        Iterator<Position> iterator = positions.iterator();
        Position previous = iterator.next();
        long time = 0L;
        while (iterator.hasNext()) {
            Position position = iterator.next();
            if (position.getSpeed() < MAX_SPEED && position.getBoolean(Position.KEY_IGNITION)) {
                time += position.getFixTime().getTime() - previous.getFixTime().getTime();
            }
            previous = position;
        }
        return time;
    }

}
