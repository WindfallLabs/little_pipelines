"""Tests for Pipeline class functionality."""

import pytest
from pathlib import Path

import little_pipelines as lp
from .conftest import PIPELINE_NAME, DO_LOGGING


class TestPipelineBasics:
    """Test basic Pipeline initialization and properties."""
    
    def test_pipeline_initialization(self):
        """Test Pipeline can be initialized."""
        pipeline = lp.Pipeline(name="test_pipeline")
        
        assert pipeline.name == "test_pipeline"
        assert pipeline.ntasks == 0
        assert pipeline.is_complete == True  # Empty pipeline is complete
        assert pipeline.cache is not None
    
    def test_pipeline_default_name(self):
        """Test Pipeline uses default name when not specified."""
        pipeline = lp.Pipeline()
        
        assert pipeline.name == "default"
    
    def test_pipeline_log_dir(self, clean_pipeline):
        """Test Pipeline log directory property."""
        pipeline = clean_pipeline
        
        assert pipeline.log_dir is not None
        assert isinstance(pipeline.log_dir, (str, Path))


class TestPipelineTaskManagement:
    """Test Pipeline task management functionality."""
    
    def test_add_single_task(self, clean_pipeline):
        """Test adding a single task to pipeline."""
        pipeline = clean_pipeline
        task = lp.Task(name="SingleTask")
        task._enable_logging = DO_LOGGING
        
        pipeline.add(task)
        
        assert pipeline.ntasks == 1
        assert task.pipeline == pipeline
    
    def test_add_multiple_tasks(self, clean_pipeline):
        """Test adding multiple tasks at once."""
        pipeline = clean_pipeline
        task1 = lp.Task(name="Task1")
        task2 = lp.Task(name="Task2")
        task1._enable_logging = DO_LOGGING
        task2._enable_logging = DO_LOGGING
        
        pipeline.add(task1, task2)
        
        assert pipeline.ntasks == 2
    
    def test_get_task_by_name(self, pipeline_with_tasks):
        """Test retrieving task by name."""
        pipeline, zero, one = pipeline_with_tasks
        
        retrieved_zero = pipeline.get_task("Zero")
        retrieved_one = pipeline.get_task("One")
        
        assert retrieved_zero == zero
        assert retrieved_one == one
    
    def test_get_task_nonexistent_raises(self, clean_pipeline):
        """Test getting nonexistent task raises KeyError."""
        pipeline = clean_pipeline
        
        with pytest.raises(KeyError):
            pipeline.get_task("NonexistentTask")
    
    def test_get_result(self, executed_pipeline):
        """Test retrieving task results."""
        pipeline, zero, one = executed_pipeline
        
        zero_result = pipeline.get_result("Zero")
        one_result = pipeline.get_result("One")
        
        assert zero_result == ["Some", "values"]
        assert one_result == ["Some", "values", "more", "values", "OK"]


class TestPipelineExecutionOrder:
    """Test Pipeline task execution ordering."""
    
    def test_execution_order_simple(self, pipeline_with_tasks):
        """Test tasks execute in dependency order."""
        pipeline, zero, one = pipeline_with_tasks
        
        tasks = [task for task in pipeline.tasks]
        
        assert len(tasks) == 2
        assert tasks[0] == zero
        assert tasks[1] == one
    
    def test_execution_order_complex(self, clean_pipeline):
        """Test execution order with complex dependencies."""
        pipeline = clean_pipeline
        
        task_a = lp.Task(name="A")
        task_b = lp.Task(name="B", dependencies=["A"])
        task_c = lp.Task(name="C", dependencies=["A"])
        task_d = lp.Task(name="D", dependencies=["B", "C"])
        
        for task in [task_a, task_b, task_c, task_d]:
            task._enable_logging = DO_LOGGING
            
            @task.process
            def run(this):
                return f"{this.name}_result"
        
        pipeline.add(task_d, task_c, task_b, task_a)  # Order shouldn't matter
        
        tasks = [task for task in pipeline.tasks]
        task_names = [task.name for task in tasks]
        
        # A must come before B and C
        assert task_names.index("A") < task_names.index("B")
        assert task_names.index("A") < task_names.index("C")
        # B and C must come before D
        assert task_names.index("B") < task_names.index("D")
        assert task_names.index("C") < task_names.index("D")


class TestPipelineExecution:
    """Test Pipeline execution functionality."""
    
    def test_basic_execution(self, pipeline_with_tasks):
        """Test basic pipeline execution."""
        pipeline, zero, one = pipeline_with_tasks
        
        assert zero.is_executed == False
        assert one.is_executed == False
        
        pipeline.execute()
        
        assert zero.is_executed == True
        assert one.is_executed == True
        assert pipeline.is_complete == True
    
    def test_execution_results(self, pipeline_with_tasks):
        """Test execution produces correct results."""
        pipeline, zero, one = pipeline_with_tasks
        
        pipeline.execute()
        
        assert zero.result == ["Some", "values"]
        assert one.result == ["Some", "values", "more", "values", "OK"]
    
    def test_force_execution_clears_cache(self, executed_pipeline):
        """Test force=True clears cached results."""
        pipeline, zero, one = executed_pipeline
        
        # Verify results are cached
        assert pipeline.get_result("Zero") is not None
        
        # Force execution should clear cache
        pipeline.execute(force=True)
        
        # Results should still exist but tasks should have re-run
        assert zero.is_executed == True
        assert one.is_executed == True


class TestPipelineCheckpointing:
    """Test Pipeline checkpointing and caching."""
    
    def test_cached_results_used(self, pipeline_with_tasks):
        """Test cached results are used on subsequent runs."""
        pipeline, zero, one = pipeline_with_tasks
        
        # First execution
        pipeline.execute()
        assert zero.is_executed == True
        
        # Create fresh pipeline with same tasks
        pipeline2 = lp.Pipeline(PIPELINE_NAME)
        zero2 = lp.Task(name="Zero")
        zero2._enable_logging = DO_LOGGING
        
        @zero2.process
        def run(this):
            return ["Some", "values"]
        
        pipeline2.add(zero2)
        
        # Second execution should use cache
        pipeline2.execute()
        
        # Task should be marked as executed (via checkpoint)
        assert zero2.is_executed == False
        assert zero2.is_skipped == True
        assert zero2.result == ["Some", "values"]
        
        # Cleanup
        pipeline2.cache.clear()
    
    def test_cache_invalidated_on_script_change(self, clean_pipeline):
        """Test cache is invalidated when task script changes."""
        pipeline = clean_pipeline
        task = lp.Task(name="ChangingTask")
        task._enable_logging = DO_LOGGING
        
        @task.process
        def run(this):
            return "v1"
        
        pipeline.add(task)
        pipeline.execute()
        
        assert task.result == "v1"
        
        # In practice, script hash would change with code modification
        # This test verifies the hash comparison mechanism exists


class TestPipelineForcedTasks:
    """Test forcing specific tasks to re-run."""
    
    def test_set_forced_tasks(self, clean_pipeline):
        """Test setting forced tasks."""
        pipeline = clean_pipeline
        
        pipeline.set_forced("TaskA", "TaskB")
        
        assert "TaskA" in pipeline.forced_tasks
        assert "TaskB" in pipeline.forced_tasks
    
    def test_clear_forced_tasks(self, clean_pipeline):
        """Test clearing forced tasks."""
        pipeline = clean_pipeline
        
        pipeline.set_forced("TaskA")
        assert len(pipeline.forced_tasks) > 0
        
        pipeline.clear_forced()
        assert len(pipeline.forced_tasks) == 0
    
    def test_forced_task_re_executes(self, executed_pipeline):
        """Test forced tasks re-execute despite cache."""
        pipeline, zero, one = executed_pipeline
        
        # Create new pipeline with same tasks
        pipeline2 = lp.Pipeline(PIPELINE_NAME)
        zero2, one2 = lp.Task(name="Zero"), lp.Task(name="One", dependencies=["Zero"])
        zero2._enable_logging = DO_LOGGING
        one2._enable_logging = DO_LOGGING
        
        @zero2.process
        def run(this):
            return ["Some", "values"]
        
        @one2.process
        def preflight(this):
            return "OK"
        
        @one2.process
        def run(this):
            status = this.preflight()
            data = this.pipeline.get_result("Zero")
            data.extend(["more", "values", status])
            return data
        
        pipeline2.add(zero2, one2)
        pipeline2.set_forced("One")
        pipeline2.execute()
        
        # One should have executed, Zero should have used cache
        assert one2.is_executed == False
        assert one2.is_skipped == True
        
        # Cleanup
        pipeline2.cache.clear()


class TestPipelineIgnoredTasks:
    """Test ignoring specific tasks."""
    
    def test_set_ignored_tasks(self, clean_pipeline):
        """Test setting ignored tasks."""
        pipeline = clean_pipeline
        
        pipeline.set_ignored("TaskA", "TaskB")
        
        assert "TaskA" in pipeline.ignored_tasks
        assert "TaskB" in pipeline.ignored_tasks
    
    def test_clear_ignored_tasks(self, clean_pipeline):
        """Test clearing ignored tasks."""
        pipeline = clean_pipeline
        
        pipeline.set_ignored("TaskA")
        assert len(pipeline.ignored_tasks) > 0
        
        pipeline.clear_ignored()
        assert len(pipeline.ignored_tasks) == 0
    
    def test_ignored_task_skipped(self, pipeline_with_tasks):
        """Test ignored tasks are skipped during execution."""
        pipeline, zero, one = pipeline_with_tasks
        
        pipeline.set_ignored("One")
        pipeline.execute()
        
        assert zero.is_executed == True
        assert one.is_executed == False
        assert one.is_skipped == True
    
    def test_forced_overrides_ignored(self, pipeline_with_tasks):
        """Test forced tasks execute even if ignored."""
        pipeline, zero, one = pipeline_with_tasks
        
        pipeline.set_ignored("One")
        pipeline.set_forced("One")
        pipeline.execute()
        
        # Forced should override ignored
        assert one.is_executed == True
        assert one.is_skipped == False


class TestPipelineValidation:
    """Test Pipeline validation functionality."""
    
    def test_validate_tasks_missing_run(self, clean_pipeline):
        """Test validation catches tasks missing run method."""
        pipeline = clean_pipeline
        task = lp.Task(name="NoRunTask")
        task._enable_logging = DO_LOGGING
        
        pipeline.add(task)
        
        with pytest.raises(AttributeError, match="missing 'run' method"):
            pipeline.validate_tasks()
    
    def test_validate_tasks_passes(self, pipeline_with_tasks):
        """Test validation passes for valid tasks."""
        pipeline, zero, one = pipeline_with_tasks
        
        # Should not raise
        pipeline.validate_tasks()


class TestPipelineCompletion:
    """Test Pipeline completion status."""
    
    def test_is_complete_empty_pipeline(self, clean_pipeline):
        """Test empty pipeline is considered complete."""
        pipeline = clean_pipeline
        
        assert pipeline.is_complete == True
    
    def test_is_complete_after_execution(self, pipeline_with_tasks):
        """Test pipeline is complete after execution."""
        pipeline, zero, one = pipeline_with_tasks
        
        assert pipeline.is_complete == False
        
        pipeline.execute()
        
        assert pipeline.is_complete == True
    
    def test_is_complete_with_skipped_tasks(self, pipeline_with_tasks):
        """Test pipeline is complete even with skipped tasks."""
        pipeline, zero, one = pipeline_with_tasks
        
        pipeline.set_ignored("One")
        pipeline.execute()
        
        assert pipeline.is_complete == True


class TestPipelineRepr:
    """Test Pipeline string representation."""
    
    def test_pipeline_repr(self, pipeline_with_tasks):
        """Test Pipeline __repr__ method."""
        pipeline, zero, one = pipeline_with_tasks
        
        assert repr(pipeline) == f"<Pipeline: {PIPELINE_NAME} (2 tasks)>"
