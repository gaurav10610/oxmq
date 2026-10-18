package io.oxmq.oxguard.connector;

import io.oxmq.oxguard.model.DriveItem;
import java.util.List;

public record DeltaSyncResult(
        List<DriveItem> updatedItems,
        List<String> deletedItemIds,
        String newDeltaToken
) {}
