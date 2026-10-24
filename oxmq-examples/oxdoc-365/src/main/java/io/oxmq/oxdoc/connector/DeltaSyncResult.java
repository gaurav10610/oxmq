package io.oxmq.oxdoc.connector;

import io.oxmq.oxdoc.model.DriveItem;
import java.util.List;

public record DeltaSyncResult(
        List<DriveItem> updatedItems,
        List<String> deletedItemIds,
        String newDeltaToken,
        String nextLink
) {
    public DeltaSyncResult(List<DriveItem> updatedItems, List<String> deletedItemIds, String newDeltaToken) {
        this(updatedItems, deletedItemIds, newDeltaToken, null);
    }

    public boolean hasMore() {
        return nextLink != null && !nextLink.isBlank();
    }
}
