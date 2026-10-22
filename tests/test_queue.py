import pytest

from harvester.models import QueueItem, Stage, StageStatus, STAGE_ORDER


class TestQueueItemStages:
    def test_stage_order(self):
        assert STAGE_ORDER[0] == Stage.DISCOVERED
        assert STAGE_ORDER[1] == Stage.INDEXED
        assert STAGE_ORDER[2] == Stage.JOB_CREATED
        assert STAGE_ORDER[3] == Stage.EXTRACTED
        assert STAGE_ORDER[4] == Stage.OCR
        assert STAGE_ORDER[5] == Stage.TRANSFORM
        assert STAGE_ORDER[6] == Stage.STORED
        assert STAGE_ORDER[7] == Stage.VALIDATED

    def test_advance_stage(self):
        item = QueueItem.__new__(QueueItem)
        item.current_stage = Stage.DISCOVERED
        result = item.advance_stage()
        assert result == Stage.INDEXED
        assert item.current_stage == Stage.INDEXED

    def test_advance_past_last(self):
        item = QueueItem.__new__(QueueItem)
        item.current_stage = Stage.VALIDATED
        result = item.advance_stage()
        assert result is None

    def test_set_stage_status(self):
        item = QueueItem.__new__(QueueItem)
        item.stage_statuses = {}
        item.set_stage_status(Stage.DISCOVERED, StageStatus.COMPLETED)
        assert item.stage_statuses["1_discovered"]["status"] == "completed"

    def test_set_stage_status_with_error(self):
        item = QueueItem.__new__(QueueItem)
        item.stage_statuses = {}
        item.set_stage_status(Stage.OCR, StageStatus.FAILED, "OCR engine crashed")
        assert item.stage_statuses["5_ocr"]["status"] == "failed"
        assert item.stage_statuses["5_ocr"]["error"] == "OCR engine crashed"
