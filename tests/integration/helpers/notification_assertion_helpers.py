def assert_no_success_and_one_failure(spy_notifier):
    """
    Verifies that there was one notification client instantiated, no success notification, and one failure notification
    """
    assert len(spy_notifier.instances) == 1

    spy_notifier_instance = spy_notifier.instances[0]
    spy_notifier_instance.success.assert_not_called()
    spy_notifier_instance.failure.assert_called_once()


def assert_success_notification(spy_notifier):
    """
    Verifies that there was one notification client instantiated, one success notification, and no failure notification
    """
    assert len(spy_notifier.instances) == 1

    spy_notifier_instance = spy_notifier.instances[0]
    spy_notifier_instance.success.assert_called_once()
    spy_notifier_instance.failure.assert_not_called()
