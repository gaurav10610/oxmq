package io.oxmq.oxdoc.connector;

import io.oxmq.oxdoc.model.DriveItem;
import java.util.List;

public record DeltaSyncResult(
        List<DriveItem> updatedItems,
        List<String> deletedItemIds,
        String newDeltaToken
) {}
